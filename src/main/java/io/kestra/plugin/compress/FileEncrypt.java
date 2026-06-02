package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    title = "Encrypt a file with AES-256",
    description = """
        Default PBKDF2_SHA256 is OpenSSL-compatible (`openssl enc -aes-256-cbc -pbkdf2`).
        Other KDFs use authenticated AES-GCM with a KESTRAENC file format."""
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
                    argon2TimeCost: 3
                    memory: 65536
                    parallelism: 1
                """
        ),
        @Example(
            title = "Encrypt with Scrypt",
            full = true,
            code = """
                id: file_encrypt_scrypt
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: encrypt
                    type: io.kestra.plugin.compress.FileEncrypt
                    from: "{{ inputs.file }}"
                    password: "{{ secret('ENCRYPTION_PASSWORD') }}"
                    keyDerivation: SCRYPT
                    memory: 65536
                    parallelism: 1
                """
        )
    }
)
public class FileEncrypt extends AbstractFileCrypt implements RunnableTask<FileEncrypt.Output> {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    @Schema(
        title = "Key derivation function",
        description = """
            PBKDF2_SHA256 (default): OpenSSL-compatible, no integrity check.
            PBKDF2_SHA512, ARGON2ID, SCRYPT: authenticated (AES-GCM)."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<KeyDerivation> keyDerivation = Property.ofValue(KeyDerivation.PBKDF2_SHA256);

    @Schema(
        title = "Memory cost",
        description = "Argon2id: memory in KB. Scrypt: N (power of 2). Ignored for PBKDF2."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> memory = Property.ofValue(65536);

    @Schema(
        title = "Parallelism",
        description = "Argon2id threads or Scrypt p. Ignored for PBKDF2."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> parallelism = Property.ofValue(1);

    @Schema(
        title = "Argon2id time cost",
        description = """
            Number of passes over the memory buffer for Argon2id.
            RFC 9106 recommends 2-4. Default 3.
            Used only when keyDerivation is ARGON2ID; ignored otherwise."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> argon2TimeCost = Property.ofValue(3);

    @Override
    public Output run(RunContext runContext) throws Exception {
        final var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        final var rIterations = runContext.render(this.iterations).as(Integer.class).orElseThrow();
        final var rMemory = runContext.render(this.memory).as(Integer.class).orElseThrow();
        final var rParallelism = runContext.render(this.parallelism).as(Integer.class).orElseThrow();
        final var rArgon2TimeCost = runContext.render(this.argon2TimeCost).as(Integer.class).orElseThrow();
        final var rKeyDerivation = runContext.render(this.keyDerivation).as(KeyDerivation.class).orElseThrow();

        final int kdfIterations = rKeyDerivation == KeyDerivation.ARGON2ID ? rArgon2TimeCost : rIterations;
        final var kdfParams = new KdfParams(rKeyDerivation, kdfIterations, rMemory, rParallelism);
        final boolean opensslFormat = rKeyDerivation == KeyDerivation.PBKDF2_SHA256;
        validateKdfParams(kdfParams);
        runContext.logger().info("Encrypting with {} ({})", rKeyDerivation, opensslFormat ? "AES-CBC" : "AES-GCM");

        final var salt = randomBytes(opensslFormat ? OPENSSL_SALT_LEN : KESTRAENC_SALT_LEN);
        final byte[] gcmNonce = opensslFormat ? null : randomBytes(GCM_NONCE_LEN);

        final char[] passChars = runContext.render(this.password).as(String.class).orElseThrow().toCharArray();
        final byte[] keyMaterial;
        try {
            keyMaterial = deriveKey(passChars, salt, kdfParams);
        } finally {
            Arrays.fill(passChars, '\0');
        }

        final Cipher cipher;
        try {
            cipher = newCipher(Cipher.ENCRYPT_MODE, keyMaterial, gcmNonce);
        } finally {
            Arrays.fill(keyMaterial, (byte) 0);
        }

        final var tempFile = runContext.workingDir().createTempFile();

        try (var out = new DataOutputStream(Files.newOutputStream(tempFile))) {
            if (opensslFormat) {
                out.write(SALTED_MAGIC);
                out.write(salt);
            } else {
                var headerBytes = buildKestraHeader(kdfParams, salt, gcmNonce);
                out.write(headerBytes);
                // Bind the header to the GCM tag so any header tamper invalidates decryption.
                cipher.updateAAD(headerBytes);
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

    private static byte[] randomBytes(int length) {
        var bytes = new byte[length];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static byte[] buildKestraHeader(KdfParams params, byte[] salt, byte[] gcmNonce) throws IOException {
        var buf = new ByteArrayOutputStream();
        writeKestraHeader(new DataOutputStream(buf), params, salt, gcmNonce);
        return buf.toByteArray();
    }

    private static void writeKestraHeader(DataOutputStream out, KdfParams params, byte[] salt, byte[] gcmNonce) throws IOException {
        out.write(KESTRAENC_MAGIC);
        out.write(KESTRAENC_VERSION);
        switch (params.algorithm()) {
            case PBKDF2_SHA512 -> {
                out.write(ALG_PBKDF2_SHA512);
                out.write(salt);
                out.write(gcmNonce);
                out.writeInt(params.iterations());
            }
            case ARGON2ID -> {
                out.write(ALG_ARGON2ID);
                out.write(salt);
                out.write(gcmNonce);
                out.writeInt(params.iterations());
                out.writeInt(params.memoryKb());
                out.writeShort(params.parallelism());
            }
            case SCRYPT -> {
                out.write(ALG_SCRYPT);
                out.write(salt);
                out.write(gcmNonce);
                out.writeInt(params.memoryKb());
                out.write(SCRYPT_R);
                out.write(params.parallelism());
            }
            case PBKDF2_SHA256 -> throw new IllegalStateException("unreachable");
        }
    }

    private static void validateKdfParams(KdfParams params) {
        switch (params.algorithm()) {
            case ARGON2ID -> {
                if (params.memoryKb() > 1_048_576)
                    throw new IllegalArgumentException("Argon2id memory exceeds 1 GiB cap: " + params.memoryKb() + " KB");
                if (params.parallelism() > 65535)
                    throw new IllegalArgumentException("Argon2id parallelism exceeds file format limit (65535): " + params.parallelism());
            }
            case SCRYPT -> {
                if (params.memoryKb() > 1_048_576)
                    throw new IllegalArgumentException("Scrypt N exceeds 1048576 cap: " + params.memoryKb());
                if (params.parallelism() > 255)
                    throw new IllegalArgumentException("Scrypt parallelism exceeds file format limit (255): " + params.parallelism());
            }
            case PBKDF2_SHA512, PBKDF2_SHA256 -> {}
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Encrypted file URI")
        private final URI uri;
    }
}
