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

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractFileCrypt extends Task {

    private static final String SALTED_MAGIC_STR = "Salted__";

    protected static byte[] saltedMagic() {
        return SALTED_MAGIC_STR.getBytes(StandardCharsets.US_ASCII);
    }

    static final byte[] KESTRAENC_MAGIC = "KESTRAENC".getBytes(StandardCharsets.US_ASCII);

    static final byte ALG_PBKDF2_SHA512 = 0x02;
    static final byte ALG_ARGON2ID      = 0x03;
    static final byte ALG_SCRYPT        = 0x04;
    static final int  SCRYPT_R          = 8;

    @Schema(title = "Internal storage URI of the source file")
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "main")
    protected Property<String> from;

    @Schema(
        title = "Encryption password",
        description = """
            Password used to derive the AES-256 key and IV.
            Use the secret() function to avoid storing the value in plain text."""
    )
    @NotNull
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> password;

    @Schema(
        title = "PBKDF2 iteration count",
        description = """
            Number of iterations for PBKDF2-HMAC-SHA256 or PBKDF2-HMAC-SHA512 key derivation.
            Also used as the iteration count for ARGON2ID.
            Must match between FileEncrypt and FileDecrypt when using PBKDF2_SHA256 (OpenSSL format).
            Minimum is 1000. Default is 100000, matching the OpenSSL 3.x default.
            Use 10000 for compatibility with OpenSSL versions prior to 3.x.
            Ignored for KESTRAENC format decryption (params are embedded in the file header)."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> iterations = Property.ofValue(100000);

    @Schema(
        title = "Key derivation function",
        description = """
            Algorithm used to derive the AES-256 key and IV from the password.
            PBKDF2_SHA256 is the default and produces OpenSSL-compatible output.
            PBKDF2_SHA512, ARGON2ID, and SCRYPT use a non-OpenSSL KESTRAENC file format.
            ARGON2ID and SCRYPT are memory-hard and GPU-resistant."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<KeyDerivation> keyDerivation = Property.ofValue(KeyDerivation.PBKDF2_SHA256);

    @Schema(
        title = "Memory cost in KB",
        description = """
            Memory cost parameter for ARGON2ID (in KB) and SCRYPT (N value).
            Ignored for PBKDF2 algorithms.
            Default is 65536 (64 MB). Higher values increase resistance to brute-force attacks."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> memory = Property.ofValue(65536);

    @Schema(
        title = "Parallelism factor",
        description = """
            Parallelism parameter for ARGON2ID (number of threads) and SCRYPT (p factor).
            Ignored for PBKDF2 algorithms.
            Default is 1."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> parallelism = Property.ofValue(1);

    static byte[] deriveKeyAndIv(char[] password, byte[] salt, KeyDerivation algorithm,
                                  int iterations, int memory, int parallelism)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        // PBKDF2_SHA256 uses AES-CBC (16-byte IV), all others use AES-GCM (12-byte nonce).
        int outputLen = (algorithm == KeyDerivation.PBKDF2_SHA256) ? 48 : 44;
        return switch (algorithm) {
            case PBKDF2_SHA256 -> derivePbkdf2(password, salt, iterations, "PBKDF2WithHmacSHA256", outputLen);
            case PBKDF2_SHA512 -> derivePbkdf2(password, salt, iterations, "PBKDF2WithHmacSHA512", outputLen);
            case ARGON2ID      -> deriveArgon2id(password, salt, iterations, memory, parallelism, outputLen);
            case SCRYPT        -> deriveScrypt(password, salt, memory, parallelism, outputLen);
        };
    }

    /**
     * Kept for backward compatibility with existing tests that call deriveKeyAndIv directly.
     * Delegates to PBKDF2_SHA256 (OpenSSL-compatible, AES-CBC) derivation producing 48 bytes.
     */
    static byte[] deriveKeyAndIv(char[] password, byte[] salt, int iterations)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        return derivePbkdf2(password, salt, iterations, "PBKDF2WithHmacSHA256", 48);
    }

    private static byte[] derivePbkdf2(char[] password, byte[] salt, int iterations, String algorithm,
                                        int outputLen)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        if (iterations < 1000) {
            throw new IllegalArgumentException("iterations must be >= 1000, got " + iterations);
        }
        var factory = SecretKeyFactory.getInstance(algorithm);
        var spec = new PBEKeySpec(password, salt, iterations, outputLen * 8);
        try {
            return factory.generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] deriveArgon2id(char[] password, byte[] salt, int iterations,
                                          int memoryKb, int parallelism, int outputLen) {
        var params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
            .withSalt(salt)
            .withIterations(iterations)
            .withMemoryAsKB(memoryKb)
            .withParallelism(parallelism)
            .build();
        var gen = new Argon2BytesGenerator();
        gen.init(params);
        var passwordBytes = new String(password).getBytes(StandardCharsets.UTF_8);
        var output = new byte[outputLen];
        gen.generateBytes(passwordBytes, output);
        Arrays.fill(passwordBytes, (byte) 0);
        return output;
    }

    private static byte[] deriveScrypt(char[] password, byte[] salt, int n, int p, int outputLen) {
        var passwordBytes = new String(password).getBytes(StandardCharsets.UTF_8);
        var output = SCrypt.generate(passwordBytes, salt, n, SCRYPT_R, p, outputLen);
        Arrays.fill(passwordBytes, (byte) 0);
        return output;
    }
}
