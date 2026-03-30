package io.kestra.plugin.compress;

import com.google.common.io.CharStreams;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
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
            .iterations(Property.ofValue(10000))
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
            .iterations(Property.ofValue(10000))
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
                .iterations(Property.ofValue(1000))
                .build();

            FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

            FileDecrypt decrypt = FileDecrypt.builder()
                .id(IdUtils.create())
                .type(FileDecrypt.class.getName())
                .from(Property.ofValue(encOut.getUri().toString()))
                .password(Property.ofValue("block-boundary-test"))
                .iterations(Property.ofValue(1000))
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
            .iterations(Property.ofValue(1000))
            .build();

        FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("wrong-password"))
            .iterations(Property.ofValue(1000))
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
            .iterations(Property.ofValue(1000))
            .build();

        FileEncrypt.Output encOut = encrypt.run(TestsUtils.mockRunContext(runContextFactory, encrypt, Map.of()));

        FileDecrypt decrypt = FileDecrypt.builder()
            .id(IdUtils.create())
            .type(FileDecrypt.class.getName())
            .from(Property.ofValue(encOut.getUri().toString()))
            .password(Property.ofValue("empty-file-test"))
            .iterations(Property.ofValue(1000))
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
            .iterations(Property.ofValue(1000))
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
            .iterations(Property.ofValue(1000))
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
            () -> AbstractFileCrypt.deriveKeyAndIv("pass".toCharArray(), new byte[8], 999)
        );
        assertThat(ex.getMessage().contains("1000"), is(true));
    }

    @Test
    void pbkdf2Sha512RoundTrip() throws Exception {
        var source = compressUtils.uploadToStorageString("PBKDF2-SHA512 test content");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("test-password"))
            .keyDerivation(Property.ofValue(KeyDerivation.PBKDF2_SHA512))
            .iterations(Property.ofValue(1000))
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
            .iterations(Property.ofValue(1))
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
    void wrongPasswordFailsArgon2id() throws Exception {
        var source = compressUtils.uploadToStorageString("secret");
        var encrypt = FileEncrypt.builder()
            .id(IdUtils.create()).type(FileEncrypt.class.getName())
            .from(Property.ofValue(source.toString()))
            .password(Property.ofValue("correct"))
            .keyDerivation(Property.ofValue(KeyDerivation.ARGON2ID))
            .iterations(Property.ofValue(1))
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
}
