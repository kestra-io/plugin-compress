package io.kestra.plugin.compress;

import com.google.common.io.CharStreams;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.compress.AbstractFileCrypt.KeyDerivation;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@KestraTest
@Execution(ExecutionMode.SAME_THREAD)
class FileEncryptTest {

    @Inject
    private CompressUtils compressUtils;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void roundTrip() throws Exception {
        URI source = compressUtils.uploadToStorageString("Hello, Kestra encryption!");

        FileEncrypt encrypt = FileEncrypt.builder()
            .id(IdUtils.create())
            .type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct-horse-battery-staple"))
            .iterations(Property.ofValue(100_000))
            .build();

        FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        String encryptedRaw = CharStreams.toString(new InputStreamReader(
            storageInterface.get(TenantService.MAIN_TENANT, null, encOut.getUri())
        ));
        assertThat(encryptedRaw, not(is("Hello, Kestra encryption!")));

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("correct-horse-battery-staple"))
            .iterations(Property.ofValue(100_000))
            .build();

        FileDecrypt.Output decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(
                storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
            )),
            is("Hello, Kestra encryption!")
        );
    }

    @Test
    void roundTripBlockBoundaries() throws Exception {
        for (int size : new int[]{32, 33, 48, 64}) {
            String content = "A".repeat(size);
            URI source = compressUtils.uploadToStorageString(content);

            FileEncrypt encrypt = FileEncrypt.builder()
                .id(IdUtils.create())
                .type(FileEncrypt.class.getName())
                .from(Property.ofValue(source.toString()))
                .password(Property.ofValue("block-boundary-test"))
                .iterations(Property.ofValue(100_000))
                .build();

            FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

            FileDecrypt decrypt = FileDecrypt.builder()
                .id(IdUtils.create())
                .type(FileDecrypt.class.getName())
                .from(Property.ofValue(encOut.getUri().toString()))
                .password(Property.ofValue("block-boundary-test"))
                .iterations(Property.ofValue(100_000))
                .build();

            FileDecrypt.Output decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

            assertThat(
                CharStreams.toString(new InputStreamReader(
                    storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
                )),
                is(content)
            );
        }
    }

    @Test
    void wrongPasswordFails() throws Exception {
        URI source = compressUtils.uploadToStorageString("secret data");

        FileEncrypt encrypt = FileEncrypt.builder()
            .id(IdUtils.create())
            .type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct-password"))
            .iterations(Property.ofValue(100_000))
            .build();

        FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("wrong-password"))
            .iterations(Property.ofValue(100_000))
            .build();

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("Decryption failed"), is(true));
    }

    @Test
    void emptyFileRoundTrip() throws Exception {
        URI source = compressUtils.uploadToStorageString("");

        FileEncrypt encrypt = FileEncrypt.builder()
            .id(IdUtils.create())
            .type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("empty-file-test"))
            .iterations(Property.ofValue(100_000))
            .build();

        FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("empty-file-test"))
            .iterations(Property.ofValue(100_000))
            .build();

        FileDecrypt.Output decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(
                storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
            )),
            is("")
        );
    }

    @Test
    void missingHeaderThrows() throws Exception {
        URI plaintext = compressUtils.uploadToStorageString("not encrypted at all");

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(plaintext.toString()))
            .password(Property.ofValue("any-password"))
            .iterations(Property.ofValue(100_000))
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("Salted__"), is(true));
    }

    @Test
    void truncatedSaltThrows() throws Exception {
        // File has valid "Salted__" magic but no salt bytes after it.
        URI truncated = compressUtils.uploadToStorageString("Salted__");

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(truncated.toString()))
            .password(Property.ofValue("any-password"))
            .iterations(Property.ofValue(100_000))
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("truncated"), is(true));
    }

    @Test
    void truncatedKestraEncHeaderThrows() throws Exception {
        // Valid KESTRAENC magic + version + algorithm byte, then EOF before the 16-byte salt
        var header = new byte[AbstractFileCrypt.KESTRAENC_MAGIC.length + 2];
        System.arraycopy(AbstractFileCrypt.KESTRAENC_MAGIC, 0, header, 0, AbstractFileCrypt.KESTRAENC_MAGIC.length);
        header[AbstractFileCrypt.KESTRAENC_MAGIC.length]     = 0x01; // version
        header[AbstractFileCrypt.KESTRAENC_MAGIC.length + 1] = AbstractFileCrypt.ALG_ARGON2ID;

        URI truncated = compressUtils.uploadToStorageBytes(header);

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(truncated.toString()))
            .password(Property.ofValue("any-password"))
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("truncated"), is(true));
    }

    @Test
    void lowIterationsThrows() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> AbstractFileCrypt.deriveKeyAndIvOpenssl("pass".toCharArray(), new byte[8], 99_999)
        );
        assertThat(ex.getMessage().contains("100000"), is(true));
    }

    @Test
    void pbkdf2Sha512RoundTrip() throws Exception {
        var source = compressUtils.uploadToStorageString("PBKDF2-SHA512 test content");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("test-password"))
            .keyDerivation(Property.ofValue(KeyDerivation.PBKDF2_SHA512))
            .iterations(Property.ofValue(100_000))
            .build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("test-password"))
            .build();
        var decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

        assertThat(CharStreams.toString(new InputStreamReader(
            storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
        )), is("PBKDF2-SHA512 test content"));
    }

    @Test
    void argon2idRoundTrip() throws Exception {
        var source = compressUtils.uploadToStorageString("Argon2id test content");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("test-password"))
            .keyDerivation(Property.ofValue(KeyDerivation.ARGON2ID))
            .argon2TimeCost(Property.ofValue(1))
            .memory(Property.ofValue(8192))
            .parallelism(Property.ofValue(1))
            .build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("test-password"))
            .build();
        var decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

        assertThat(CharStreams.toString(new InputStreamReader(
            storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
        )), is("Argon2id test content"));
    }

    @Test
    void scryptRoundTrip() throws Exception {
        var source = compressUtils.uploadToStorageString("scrypt test content");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("test-password"))
            .keyDerivation(Property.ofValue(KeyDerivation.SCRYPT))
            .memory(Property.ofValue(1024))
            .parallelism(Property.ofValue(1))
            .build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("test-password"))
            .build();
        var decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

        assertThat(CharStreams.toString(new InputStreamReader(
            storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
        )), is("scrypt test content"));
    }

    @Test
    void opensslDecryptsFileEncryptOutput() throws Exception {
        assumeTrue(opensslAvailable(), "openssl binary not on PATH");
        String content = "kestra <-> openssl cross-compat test";
        String password = "shared-secret-123";
        int iterations = 100_000;
        URI source = compressUtils.uploadToStorageString(content);

        FileEncrypt encrypt = FileEncrypt.builder()
            .id(IdUtils.create())
            .type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue(password))
            .iterations(Property.ofValue(iterations))
            .build();
        FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        Path encryptedFile = Files.createTempFile("kestra-enc-", ".bin");
        Path decryptedFile = Files.createTempFile("kestra-dec-", ".txt");
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, encOut.getUri())) {
            Files.copy(in, encryptedFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }

        Process p = new ProcessBuilder(
            "openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2",
            "-iter", String.valueOf(iterations),
            "-in", encryptedFile.toString(),
            "-out", decryptedFile.toString(),
            "-pass", "pass:" + password
        ).redirectErrorStream(true).start();
        String stderr = new String(p.getInputStream().readAllBytes());
        int exit = p.waitFor();
        assertThat("openssl failed: " + stderr, exit, is(0));
        assertThat(Files.readString(decryptedFile), is(content));

        Files.deleteIfExists(encryptedFile);
        Files.deleteIfExists(decryptedFile);
    }

    @Test
    void fileDecryptReadsOpensslOutput() throws Exception {
        assumeTrue(opensslAvailable(), "openssl binary not on PATH");
        String content = "openssl -> kestra cross-compat test";
        String password = "shared-secret-456";
        int iterations = 100_000;

        Path plaintextFile = Files.createTempFile("kestra-plain-", ".txt");
        Path encryptedFile = Files.createTempFile("kestra-osslenc-", ".bin");
        Files.writeString(plaintextFile, content);

        Process p = new ProcessBuilder(
            "openssl", "enc", "-aes-256-cbc", "-pbkdf2",
            "-iter", String.valueOf(iterations),
            "-in", plaintextFile.toString(),
            "-out", encryptedFile.toString(),
            "-pass", "pass:" + password
        ).redirectErrorStream(true).start();
        String stderr = new String(p.getInputStream().readAllBytes());
        int exit = p.waitFor();
        assertThat("openssl failed: " + stderr, exit, is(0));

        URI encryptedUri = compressUtils.uploadToStorageBytes(Files.readAllBytes(encryptedFile));
        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(encryptedUri.toString()))
            .password(Property.ofValue(password))
            .iterations(Property.ofValue(iterations))
            .build();
        FileDecrypt.Output decOut = decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(
                storageInterface.get(TenantService.MAIN_TENANT, null, decOut.getUri())
            )),
            is(content)
        );

        Files.deleteIfExists(plaintextFile);
        Files.deleteIfExists(encryptedFile);
    }

    private static boolean opensslAvailable() {
        try {
            Process p = new ProcessBuilder("openssl", "version").redirectErrorStream(true).start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void wrongPasswordFailsArgon2id() throws Exception {
        var source = compressUtils.uploadToStorageString("secret");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct"))
            .keyDerivation(Property.ofValue(KeyDerivation.ARGON2ID))
            .argon2TimeCost(Property.ofValue(1))
            .memory(Property.ofValue(8192))
            .parallelism(Property.ofValue(1))
            .build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("wrong"))
            .build();

        var ex = assertThrows(IllegalStateException.class, () ->
            decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("Decryption failed"), is(true));
    }

    @Test
    void wrongPasswordFailsPbkdf2Sha512() throws Exception {
        var source = compressUtils.uploadToStorageString("secret");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct"))
            .keyDerivation(Property.ofValue(KeyDerivation.PBKDF2_SHA512))
            .iterations(Property.ofValue(100_000))
            .build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("wrong"))
            .build();

        var ex = assertThrows(IllegalStateException.class, () ->
            decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("Decryption failed"), is(true));
    }

    @Test
    void wrongPasswordFailsScrypt() throws Exception {
        var source = compressUtils.uploadToStorageString("secret");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct"))
            .keyDerivation(Property.ofValue(KeyDerivation.SCRYPT))
            .memory(Property.ofValue(1024))
            .parallelism(Property.ofValue(1))
            .build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("wrong"))
            .build();

        var ex = assertThrows(IllegalStateException.class, () ->
            decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("Decryption failed"), is(true));
    }

    @Test
    void tamperedGcmCiphertextFails() throws Exception {
        assertTamperFails(KeyDerivation.ARGON2ID, b -> b.argon2TimeCost(Property.ofValue(1)).memory(Property.ofValue(8192)).parallelism(Property.ofValue(1)));
    }

    @Test
    void tamperedPbkdf2Sha512CiphertextFails() throws Exception {
        assertTamperFails(KeyDerivation.PBKDF2_SHA512, b -> b.iterations(Property.ofValue(100_000)));
    }

    @Test
    void tamperedScryptCiphertextFails() throws Exception {
        assertTamperFails(KeyDerivation.SCRYPT, b -> b.memory(Property.ofValue(1024)).parallelism(Property.ofValue(1)));
    }

    private void assertTamperFails(KeyDerivation kdf, java.util.function.Consumer<FileEncrypt.FileEncryptBuilder<?, ?>> configure) throws Exception {
        var source = compressUtils.uploadToStorageString("secret payload");
        var builder = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct"))
            .keyDerivation(Property.ofValue(kdf));
        configure.accept(builder);
        var encrypt = builder.build();
        var encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        var bytes = storageInterface.get(TenantService.MAIN_TENANT, null, encOut.getUri()).readAllBytes();
        bytes[bytes.length - 1] ^= 0x01;
        var tampered = Path.of(System.getProperty("java.io.tmpdir"), "tampered-" + IdUtils.create());
        Files.write(tampered, bytes);
        var tamperedUri = storageInterface.put(TenantService.MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".bin"), Files.newInputStream(tampered));

        var decrypt = FileDecrypt.builder()
            .id(IdUtils.create()).type(FileDecrypt.class.getName())
            .from(Property.ofValue(tamperedUri.toString()))
            .password(Property.ofValue("correct"))
            .build();

        var ex = assertThrows(IllegalStateException.class, () ->
            decrypt.run(TestsUtils.mockRunContext(runContextFactory, decrypt, Map.of()))
        );
        assertThat(ex.getMessage().contains("Decryption failed"), is(true));
    }
}
