package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.DataOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Arrays;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Encrypt a file with AES-256 and a password",
    description = """
    Encrypts a file using AES-256. The default algorithm (PBKDF2_SHA256) uses AES-CBC and produces
    output compatible with `openssl enc -aes-256-cbc -pbkdf2 -iter <count>`.
    Other algorithms (PBKDF2_SHA512, ARGON2ID, SCRYPT) use AES-GCM with a KESTRAENC file format that
    embeds all derivation parameters in the header, so FileDecrypt requires no extra configuration.
    AES-GCM provides built-in integrity protection. AES-CBC (PBKDF2_SHA256) does not."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: file_encrypt
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: encrypt
                    type: io.kestra.plugin.compress.FileEncrypt
                    from: "{{ inputs.file }}"
                    password: "{{ secret('ENCRYPTION_PASSWORD') }}"
                    iterations: 10000
                """
        ),
        @Example(
            title = "Encrypt with Argon2id (memory-hard, GPU-resistant)",
            full = true,
            code = """
                id: file_encrypt_argon2id
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: encrypt
                    type: io.kestra.plugin.compress.FileEncrypt
                    from: "{{ inputs.file }}"
                    password: "{{ secret('ENCRYPTION_PASSWORD') }}"
                    keyDerivation: ARGON2ID
                    iterations: 3
                    memory: 65536
                    parallelism: 1
                """
        )
    }
)
public class FileEncrypt extends AbstractFileCrypt implements RunnableTask<FileEncrypt.Output> {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        var rIterations = runContext.render(this.iterations).as(Integer.class).orElseThrow();
        var rMemory = runContext.render(this.memory).as(Integer.class).orElseThrow();
        var rParallelism = runContext.render(this.parallelism).as(Integer.class).orElseThrow();
        var rKeyDerivation = runContext.render(this.keyDerivation).as(KeyDerivation.class).orElseThrow();

        boolean opensslFormat = rKeyDerivation == KeyDerivation.PBKDF2_SHA256;
        int saltLen = opensslFormat ? 8 : 16;
        var salt = new byte[saltLen];
        SECURE_RANDOM.nextBytes(salt);

        char[] passChars = runContext.render(this.password).as(String.class).orElseThrow().toCharArray();
        byte[] keyMaterial;
        try {
            keyMaterial = deriveKeyAndIv(passChars, salt, rKeyDerivation, rIterations, rMemory, rParallelism);
        } finally {
            Arrays.fill(passChars, '\0');
        }
        var secretKey = new SecretKeySpec(keyMaterial, 0, KEY_LEN, "AES");

        Cipher cipher;
        if (opensslFormat) {
            var cipherIv = new IvParameterSpec(keyMaterial, KEY_LEN, CBC_IV_LEN);
            Arrays.fill(keyMaterial, (byte) 0);
            // CBC required: OpenSSL enc uses CBC and cannot decrypt GCM output
            // No padding oracle risk: files at rest have no oracle for attackers to interact with
            cipher = Cipher.getInstance(CIPHER_CBC);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, cipherIv);
        } else {
            var gcmNonce = Arrays.copyOfRange(keyMaterial, KEY_LEN, KEY_LEN + GCM_NONCE_LEN);
            Arrays.fill(keyMaterial, (byte) 0);
            cipher = Cipher.getInstance(CIPHER_GCM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, gcmNonce));
        }

        var tempFile = runContext.workingDir().createTempFile();

        try (var out = new DataOutputStream(Files.newOutputStream(tempFile))) {
            if (opensslFormat) {
                out.write(SALTED_MAGIC);
                out.write(salt);
            } else {
                out.write(KESTRAENC_MAGIC);
                out.write(KESTRAENC_VERSION);
                byte algorithmId = switch (rKeyDerivation) {
                    case PBKDF2_SHA512 -> ALG_PBKDF2_SHA512;
                    case ARGON2ID -> ALG_ARGON2ID;
                    case SCRYPT -> ALG_SCRYPT;
                    default -> throw new IllegalStateException("unexpected algorithm: " + rKeyDerivation);
                };
                out.write(algorithmId);
                out.write(salt);
                switch (rKeyDerivation) {
                    case PBKDF2_SHA512 -> out.writeInt(rIterations);
                    case ARGON2ID -> { out.writeInt(rIterations); out.writeInt(rMemory); out.writeShort(rParallelism); }
                    case SCRYPT -> {
                        if (rParallelism < 1 || rParallelism > 255)
                            throw new IllegalArgumentException("SCRYPT parallelism must be between 1 and 255, got " + rParallelism);
                        out.writeInt(rMemory);
                        out.write(SCRYPT_R);
                        out.write(rParallelism);
                    }
                    default -> throw new IllegalStateException("unexpected algorithm: " + rKeyDerivation);
                }
            }
            try (
                var in = runContext.storage().getFile(URI.create(rFrom));
                var cipherOut = new CipherOutputStream(out, cipher)
            ) {
                in.transferTo(cipherOut);
            }
        }

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile.toFile()))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the encrypted file on Kestra's internal storage")
        private final URI uri;
    }
}
