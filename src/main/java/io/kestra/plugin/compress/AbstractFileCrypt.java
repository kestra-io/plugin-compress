package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.params.Argon2Parameters;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractFileCrypt extends Task {

    static final byte[] SALTED_MAGIC   = "Salted__".getBytes(StandardCharsets.US_ASCII);
    static final byte[] KESTRAENC_MAGIC = "KESTRAENC".getBytes(StandardCharsets.US_ASCII);

    static final byte KESTRAENC_VERSION  = 0x01;
    static final byte ALG_PBKDF2_SHA512  = 0x02;
    static final byte ALG_ARGON2ID       = 0x03;
    static final byte ALG_SCRYPT         = 0x04;
    static final int  SCRYPT_R           = 8;

    static final String PBKDF2_SHA256_ALG = "PBKDF2WithHmacSHA256";
    static final String PBKDF2_SHA512_ALG = "PBKDF2WithHmacSHA512";
    static final int    KEY_LEN          = 32;
    static final int    CBC_IV_LEN       = 16;
    static final int    GCM_NONCE_LEN    = 12;
    static final int    OPENSSL_SALT_LEN = 8;
    static final int    KESTRAENC_SALT_LEN = 16;
    static final int    MIN_PBKDF2_ITERATIONS = 10000;

    @Schema(title = "Source file URI")
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "main")
    protected Property<String> from;

    @Schema(title = "Password")
    @NotNull
    @ToString.Exclude
    @PluginProperty(secret = true, group = "main")
    protected Property<String> password;

    @Schema(
        title = "Iteration count",
        description = """
            PBKDF2: number of hashing rounds (min 10000, default 600000).
            Argon2id: time cost. Scrypt: ignored.
            Ignored when decrypting KESTRAENC files."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> iterations = Property.ofValue(600000);

    enum KeyDerivation { PBKDF2_SHA256, PBKDF2_SHA512, ARGON2ID, SCRYPT }

    record KdfParams(KeyDerivation algorithm, int iterations, int memoryKb, int parallelism) {
        static KdfParams pbkdf2(KeyDerivation algorithm, int iterations) {
            return new KdfParams(algorithm, iterations, 0, 0);
        }

        static KdfParams argon2id(int iterations, int memoryKb, int parallelism) {
            return new KdfParams(KeyDerivation.ARGON2ID, iterations, memoryKb, parallelism);
        }

        static KdfParams scrypt(int memoryKb, int parallelism) {
            return new KdfParams(KeyDerivation.SCRYPT, 0, memoryKb, parallelism);
        }
    }

    record CipherInit(byte[] keyMaterial, byte[] gcmNonce) {}

    static Cipher newCipher(int mode, byte[] keyMaterial, byte[] gcmNonce) throws GeneralSecurityException {
        var secretKey = new SecretKeySpec(keyMaterial, 0, KEY_LEN, "AES");
        if (gcmNonce == null) {
            // CBC is intentional: OpenSSL enc uses CBC and cannot decrypt GCM output.
            // No padding oracle risk: files at rest have no oracle for attackers to interact with.
            var cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(mode, secretKey, new IvParameterSpec(keyMaterial, KEY_LEN, CBC_IV_LEN));
            return cipher;
        }
        var cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, secretKey, new GCMParameterSpec(128, gcmNonce));
        return cipher;
    }

    static byte[] deriveKey(char[] password, byte[] salt, KdfParams params) throws NoSuchAlgorithmException, InvalidKeySpecException {
        final int iter = params.iterations();
        final int mem = params.memoryKb();
        final int par = params.parallelism();
        return switch (params.algorithm()) {
            case PBKDF2_SHA256 -> derivePbkdf2(password, salt, iter, PBKDF2_SHA256_ALG, KEY_LEN + CBC_IV_LEN);
            case PBKDF2_SHA512 -> derivePbkdf2(password, salt, iter, PBKDF2_SHA512_ALG, KEY_LEN);
            case ARGON2ID -> deriveArgon2id(password, salt, iter, mem, par, KEY_LEN);
            case SCRYPT -> deriveScrypt(password, salt, mem, par, KEY_LEN);
        };
    }

    static byte[] deriveKeyAndIvOpenssl(char[] password, byte[] salt, int iterations) throws NoSuchAlgorithmException, InvalidKeySpecException {
        return derivePbkdf2(password, salt, iterations, PBKDF2_SHA256_ALG, KEY_LEN + CBC_IV_LEN);
    }

    private static byte[] derivePbkdf2(char[] password, byte[] salt, int iterations, String algorithm, int outputLen) throws NoSuchAlgorithmException, InvalidKeySpecException {
        if (iterations < MIN_PBKDF2_ITERATIONS) {
            throw new IllegalArgumentException("iterations must be >= " + MIN_PBKDF2_ITERATIONS + ", got " + iterations);
        }
        var spec = new PBEKeySpec(password, salt, iterations, outputLen * 8);
        try {
            return SecretKeyFactory.getInstance(algorithm).generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] deriveArgon2id(char[] password, byte[] salt, int iterations, int memoryKb, int parallelism, int outputLen) {
        var gen = new Argon2BytesGenerator();
        gen.init(new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
            .withSalt(salt).withIterations(iterations).withMemoryAsKB(memoryKb).withParallelism(parallelism).build());
        var passwordBytes = toUtf8Bytes(password);
        var output = new byte[outputLen];
        gen.generateBytes(passwordBytes, output);
        Arrays.fill(passwordBytes, (byte) 0);
        return output;
    }

    private static byte[] deriveScrypt(char[] password, byte[] salt, int n, int p, int outputLen) {
        var passwordBytes = toUtf8Bytes(password);
        var result = SCrypt.generate(passwordBytes, salt, n, SCRYPT_R, p, outputLen);
        Arrays.fill(passwordBytes, (byte) 0);
        return result;
    }

    private static byte[] toUtf8Bytes(char[] password) {
        var buf = StandardCharsets.UTF_8.encode(CharBuffer.wrap(password));
        var bytes = new byte[buf.limit()];
        buf.get(bytes);
        Arrays.fill(buf.array(), (byte) 0);
        return bytes;
    }
}
