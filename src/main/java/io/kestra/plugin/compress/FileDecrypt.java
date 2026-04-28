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
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.DataInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Decrypt a file encrypted with AES-256 and a password",
    description = """
    Decrypts a file produced by FileEncrypt or `openssl enc -aes-256-cbc -pbkdf2 -iter <count>`.
    Automatically detects the file format from the header: 'Salted__' (OpenSSL/PBKDF2_SHA256, AES-CBC)
    or 'KESTRAENC' (PBKDF2_SHA512, ARGON2ID, SCRYPT, AES-GCM). For KESTRAENC files, all derivation
    parameters are read from the header, so only the password is required."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: file_decrypt
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: decrypt
                    type: io.kestra.plugin.compress.FileDecrypt
                    from: "{{ inputs.file }}"
                    password: "{{ secret('ENCRYPTION_PASSWORD') }}"
                    iterations: 10000
                """
        ),
        @Example(
            title = "Decrypt a KESTRAENC file (any non-PBKDF2_SHA256 algorithm)",
            full = true,
            code = """
                id: file_decrypt_argon2id
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: decrypt
                    type: io.kestra.plugin.compress.FileDecrypt
                    from: "{{ inputs.file }}"
                    password: "{{ secret('ENCRYPTION_PASSWORD') }}"
                """
        )
    }
)
public class FileDecrypt extends AbstractFileCrypt implements RunnableTask<FileDecrypt.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        var rIterations = runContext.render(this.iterations).as(Integer.class).orElseThrow();

        char[] passChars = runContext.render(this.password).as(String.class).orElseThrow().toCharArray();

        try (var raw = runContext.storage().getFile(URI.create(rFrom))) {
            var headerBytes = raw.readNBytes(KESTRAENC_MAGIC.length);

            var tempFile = runContext.workingDir().createTempFile();

            try {
                if (headerBytes.length == KESTRAENC_MAGIC.length && Arrays.equals(headerBytes, KESTRAENC_MAGIC)) {
                    decryptKestraFormat(raw, passChars, tempFile);
                } else if (headerBytes.length >= 8 && Arrays.equals(Arrays.copyOf(headerBytes, 8), saltedMagic())) {
                    decryptOpensslFormat(raw, headerBytes, rIterations, passChars, tempFile);
                } else {
                    throw new IllegalArgumentException(
                        "Unknown file format: expected 'Salted__' (OpenSSL) or 'KESTRAENC' header. " +
                        "Ensure the file was encrypted with FileEncrypt."
                    );
                }
            } finally {
                Arrays.fill(passChars, '\0');
            }

            return Output.builder()
                .uri(runContext.storage().putFile(tempFile.toFile()))
                .build();
        }
    }

    private static void decryptOpensslFormat(InputStream raw, byte[] headerBytes, int iterations,
                                              char[] passChars, Path tempFile) throws Exception {
        int saltBytesAlreadyRead = headerBytes.length - 8;
        int saltBytesNeeded = 8 - saltBytesAlreadyRead;
        var saltRemainder = raw.readNBytes(saltBytesNeeded);
        if (saltRemainder.length != saltBytesNeeded) {
            throw new IllegalArgumentException(
                "Input file is truncated: expected 8-byte salt after 'Salted__' header."
            );
        }
        var salt = new byte[8];
        if (saltBytesAlreadyRead > 0) {
            salt[0] = headerBytes[8];
        }
        System.arraycopy(saltRemainder, 0, salt, saltBytesAlreadyRead, saltBytesNeeded);

        var keyMaterial = deriveKeyAndIv(passChars, salt, KeyDerivation.PBKDF2_SHA256, iterations, 0, 0);
        decrypt(keyMaterial, KeyDerivation.PBKDF2_SHA256, raw, tempFile);
    }

    private static void decryptKestraFormat(InputStream raw, char[] passChars, Path tempFile) throws Exception {
        var dis = new DataInputStream(raw);
        var version = dis.read();
        if (version != 0x01) {
            throw new IllegalArgumentException("Unsupported KESTRAENC format version: " + version);
        }
        var algorithmId = (byte) dis.read();
        var salt = dis.readNBytes(16);
        if (salt.length != 16) {
            throw new IllegalArgumentException("Input file is truncated: expected 16-byte salt.");
        }
        switch (algorithmId) {
            case ALG_PBKDF2_SHA512 -> {
                var iterations = dis.readInt();
                if (iterations < 1000 || iterations > 10_000_000)
                    throw new IllegalArgumentException("KESTRAENC: PBKDF2 iterations out of range: " + iterations);
                decrypt(deriveKeyAndIv(passChars, salt, KeyDerivation.PBKDF2_SHA512, iterations, 0, 0), KeyDerivation.PBKDF2_SHA512, dis, tempFile);
            }
            case ALG_ARGON2ID -> {
                var iterations = dis.readInt();
                var memoryKb = dis.readInt();
                var parallelism = dis.readUnsignedShort();
                if (iterations < 1 || iterations > 10_000_000)
                    throw new IllegalArgumentException("KESTRAENC: Argon2id iterations out of range: " + iterations);
                if (memoryKb < 8 || memoryKb > 1_048_576)
                    throw new IllegalArgumentException("KESTRAENC: Argon2id memory out of range: " + memoryKb);
                if (parallelism < 1 || parallelism > 64)
                    throw new IllegalArgumentException("KESTRAENC: Argon2id parallelism out of range: " + parallelism);
                decrypt(deriveKeyAndIv(passChars, salt, KeyDerivation.ARGON2ID, iterations, memoryKb, parallelism), KeyDerivation.ARGON2ID, dis, tempFile);
            }
            case ALG_SCRYPT -> {
                var memoryKb = dis.readInt();
                dis.read(); // SCRYPT_R byte (fixed at 8, not stored)
                var parallelism = dis.read();
                if (memoryKb < 2 || memoryKb > 1_048_576)
                    throw new IllegalArgumentException("KESTRAENC: scrypt N out of range: " + memoryKb);
                if (parallelism < 1 || parallelism > 255)
                    throw new IllegalArgumentException("KESTRAENC: scrypt p out of range: " + parallelism);
                decrypt(deriveKeyAndIv(passChars, salt, KeyDerivation.SCRYPT, 0, memoryKb, parallelism), KeyDerivation.SCRYPT, dis, tempFile);
            }
            default -> throw new IllegalArgumentException("Unknown KDF algorithm byte: 0x" + Integer.toHexString(algorithmId & 0xFF));
        }
    }

    private static void decrypt(byte[] keyMaterial, KeyDerivation algorithm, InputStream raw,
                                 Path tempFile) throws Exception {
        try {
            var secretKey = new SecretKeySpec(keyMaterial, 0, 32, "AES");
            Cipher cipher;
            if (algorithm == KeyDerivation.PBKDF2_SHA256) {
                var cipherIv = new IvParameterSpec(keyMaterial, 32, 16);
                // CBC required: OpenSSL enc uses CBC and cannot decrypt GCM output
                // No padding oracle risk: files at rest have no oracle for attackers to interact with
                cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                cipher.init(Cipher.DECRYPT_MODE, secretKey, cipherIv);
            } else {
                var gcmNonce = Arrays.copyOfRange(keyMaterial, 32, 44);
                cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(128, gcmNonce));
            }
            try (var out = Files.newOutputStream(tempFile)) {
                var buffer = new byte[8192];
                int read;
                while ((read = raw.read(buffer)) != -1) {
                    var decryptedChunk = cipher.update(buffer, 0, read);
                    if (decryptedChunk != null) out.write(decryptedChunk);
                }
                try {
                    var remainingBytes = cipher.doFinal();
                    out.write(remainingBytes);
                } catch (javax.crypto.BadPaddingException | javax.crypto.IllegalBlockSizeException e) {
                    throw new IllegalStateException("Decryption failed: incorrect password or corrupted file", e);
                }
            }
        } finally {
            Arrays.fill(keyMaterial, (byte) 0);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "URI of the decrypted file on Kestra's internal storage")
        private final URI uri;
    }
}
