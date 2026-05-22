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

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
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
    title = "Decrypt a file encrypted with AES-256",
    description = """
        Detects the file format automatically (OpenSSL or KESTRAENC).
        For OpenSSL files, the iterations property must match the value used at encrypt time."""
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
                """
        )
    }
)
public class FileDecrypt extends AbstractFileCrypt implements RunnableTask<FileDecrypt.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        final var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        final var rIterations = runContext.render(this.iterations).as(Integer.class).orElseThrow();

        runContext.logger().info("Decrypting {}", rFrom);

        final char[] passChars = runContext.render(this.password).as(String.class).orElseThrow().toCharArray();
        try (var raw = runContext.storage().getFile(URI.create(rFrom))) {
            final var tempFile = runContext.workingDir().createTempFile();
            final var header = raw.readNBytes(SALTED_MAGIC.length);

            if (Arrays.equals(header, SALTED_MAGIC)) {
                decryptOpensslFormat(raw, rIterations, passChars, tempFile);
            } else if (isKestraEncHeader(header, raw)) {
                decryptKestraFormat(raw, passChars, tempFile);
            } else {
                throw new IllegalArgumentException(
                    "Unknown file format: expected 'Salted__' (OpenSSL) or 'KESTRAENC' header. " +
                    "Ensure the file was encrypted with FileEncrypt."
                );
            }

            return Output.builder()
                .uri(runContext.storage().putFile(tempFile.toFile()))
                .build();
        } finally {
            Arrays.fill(passChars, '\0');
        }
    }

    private static boolean isKestraEncHeader(byte[] firstBytes, InputStream raw) throws IOException {
        if (firstBytes.length != SALTED_MAGIC.length) return false;
        for (int i = 0; i < SALTED_MAGIC.length; i++) {
            if (firstBytes[i] != KESTRAENC_MAGIC[i]) return false;
        }
        return raw.read() == (KESTRAENC_MAGIC[KESTRAENC_MAGIC.length - 1] & 0xFF);
    }

    private static void decryptOpensslFormat(InputStream raw, int iterations, char[] passChars, Path tempFile) throws Exception {
        var salt = raw.readNBytes(OPENSSL_SALT_LEN);
        if (salt.length != OPENSSL_SALT_LEN)
            throw new IllegalArgumentException("Input file is truncated: expected " + OPENSSL_SALT_LEN + "-byte salt after 'Salted__' header.");
        decrypt(new CipherInit(deriveKeyAndIvOpenssl(passChars, salt, iterations), null), raw, tempFile, null);
    }

    private static void decryptKestraFormat(InputStream raw, char[] passChars, Path tempFile) throws Exception {
        var capturing = new CapturingInputStream(raw);
        var dis = new DataInputStream(capturing);
        var version = dis.read();
        if (version != KESTRAENC_VERSION) {
            throw new IllegalArgumentException("Unsupported KESTRAENC format version: " + version);
        }
        var algorithmId = (byte) dis.read();
        var salt = dis.readNBytes(KESTRAENC_SALT_LEN);
        if (salt.length != KESTRAENC_SALT_LEN) {
            throw new IllegalArgumentException("Input file is truncated: expected " + KESTRAENC_SALT_LEN + "-byte salt.");
        }
        var nonce = dis.readNBytes(GCM_NONCE_LEN);
        if (nonce.length != GCM_NONCE_LEN) {
            throw new IllegalArgumentException("Input file is truncated: expected " + GCM_NONCE_LEN + "-byte GCM nonce.");
        }
        KdfParams params = switch (algorithmId) {
            case ALG_PBKDF2_SHA512 -> {
                var iterations = dis.readInt();
                if (iterations < MIN_PBKDF2_ITERATIONS || iterations > 10_000_000)
                    throw new IllegalArgumentException("KESTRAENC: PBKDF2 iterations out of range [" + MIN_PBKDF2_ITERATIONS + ", 10000000]: " + iterations);
                yield KdfParams.pbkdf2(KeyDerivation.PBKDF2_SHA512, iterations);
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
                yield KdfParams.argon2id(iterations, memoryKb, parallelism);
            }
            case ALG_SCRYPT -> {
                var memoryKb = dis.readInt();
                int rByte = dis.read();
                if (rByte != SCRYPT_R) {
                    throw new IllegalArgumentException("KESTRAENC: unsupported scrypt r parameter: " + rByte + ", expected " + SCRYPT_R);
                }
                var parallelism = dis.read();
                if (memoryKb < 2 || memoryKb > 1_048_576 || (memoryKb & (memoryKb - 1)) != 0)
                    throw new IllegalArgumentException("KESTRAENC: scrypt N must be a power of 2 in [2, 1048576], got " + memoryKb);
                if (parallelism < 1 || parallelism > 255)
                    throw new IllegalArgumentException("KESTRAENC: scrypt p out of range: " + parallelism);
                yield KdfParams.scrypt(memoryKb, parallelism);
            }
            default -> throw new IllegalArgumentException("Unknown KDF algorithm byte: 0x" + Integer.toHexString(algorithmId & 0xFF));
        };

        var aad = capturing.captured(KESTRAENC_MAGIC);
        decrypt(new CipherInit(deriveKey(passChars, salt, params), nonce), dis, tempFile, aad);
    }

    private static final class CapturingInputStream extends FilterInputStream {
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        CapturingInputStream(InputStream in) { super(in); }

        @Override public int read() throws IOException {
            var b = super.read();
            if (b != -1) buf.write(b);
            return b;
        }

        @Override public int read(byte[] bytes, int off, int len) throws IOException {
            var n = super.read(bytes, off, len);
            if (n > 0) buf.write(bytes, off, n);
            return n;
        }

        byte[] captured(byte[] prefix) {
            var captured = buf.toByteArray();
            var out = new byte[prefix.length + captured.length];
            System.arraycopy(prefix, 0, out, 0, prefix.length);
            System.arraycopy(captured, 0, out, prefix.length, captured.length);
            return out;
        }
    }

    private static void decrypt(CipherInit init, InputStream raw, Path tempFile, byte[] aad) throws Exception {
        final var keyMaterial = init.keyMaterial();
        final Cipher cipher;
        try {
            cipher = newCipher(Cipher.DECRYPT_MODE, keyMaterial, init.gcmNonce());
        } finally {
            Arrays.fill(keyMaterial, (byte) 0);
        }
        if (aad != null) {
            cipher.updateAAD(aad);
        }
        try (
            var out = Files.newOutputStream(tempFile);
            var cipherIn = new CipherInputStream(raw, cipher)
        ) {
            cipherIn.transferTo(out);
        } catch (IOException e) {
            if (e.getCause() instanceof BadPaddingException || e.getCause() instanceof IllegalBlockSizeException) {
                throw new IllegalStateException("Decryption failed: incorrect password or corrupted file", e.getCause());
            }
            throw e;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Decrypted file URI")
        private final URI uri;
    }
}
