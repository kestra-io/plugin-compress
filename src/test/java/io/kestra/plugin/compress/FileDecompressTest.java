package io.kestra.plugin.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;

import com.google.common.io.CharStreams;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class FileDecompressTest {
    @Inject
    private CompressUtils compressUtils;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.BROTLI, "1.txt.br"),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.Z, "1.txt.Z")
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void suite(ArchiveDecompress.CompressionAlgorithm compression, String resource) throws Exception {
        URI uri = compressUtils.uploadToStorage("decompress/" + resource);

        FileDecompress decompress = FileDecompress.builder()
            .id("unit-test")
            .type(ArchiveDecompress.class.getName())
            .compression(Property.ofValue(compression))
            .from(Property.ofValue(uri.toString()))
            .build();

        FileDecompress.Output runDecompress = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of()));

        assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runDecompress.getUri()))), is("1"));
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveDecompress.CompressionAlgorithm.class, names = {"GZIP", "BZIP2", "XZ", "LZ4FRAME"})
    void decompressesEveryMemberOfAConcatenatedFile(ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        // like `cat a.gz b.gz > both.gz`: two complete compressed members, one after the other
        var both = new ByteArrayOutputStream();
        both.writeBytes(member(compression, "first\n"));
        both.writeBytes(member(compression, "second\n"));

        assertThat(decompress(compression, both.toByteArray()), is("first\nsecond\n"));
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveDecompress.CompressionAlgorithm.class, names = {"GZIP", "BZIP2", "XZ", "LZ4FRAME"})
    void ignoresTrailingZeroPaddingAfterTheLastMember(ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        // tape and block writers pad files with zero bytes, GNU gzip ignores them after a valid member
        var padded = new ByteArrayOutputStream();
        padded.writeBytes(member(compression, "first\n"));
        padded.writeBytes(member(compression, "second\n"));
        padded.writeBytes(new byte[512]);

        assertThat(decompress(compression, padded.toByteArray()), is("first\nsecond\n"));
    }

    @Test
    void decompressesATarGzWithTrailingZeroPadding() throws Exception {
        var tar = new ByteArrayOutputStream();
        try (var out = new TarArchiveOutputStream(tar)) {
            var content = "1".getBytes(StandardCharsets.UTF_8);
            var entry = new TarArchiveEntry("folder/1.txt");
            entry.setSize(content.length);
            out.putArchiveEntry(entry);
            out.write(content);
            out.closeArchiveEntry();
        }
        var padded = new ByteArrayOutputStream();
        padded.writeBytes(member(ArchiveDecompress.CompressionAlgorithm.GZIP, tar.toByteArray()));
        padded.writeBytes(new byte[1024]);
        var uri = compressUtils.uploadToStorageBytes(padded.toByteArray());

        var decompress = ArchiveDecompress.builder()
            .id(IdUtils.create())
            .type(ArchiveDecompress.class.getName())
            .algorithm(Property.ofValue(ArchiveDecompress.ArchiveAlgorithm.TAR))
            .compression(Property.ofValue(ArchiveDecompress.CompressionAlgorithm.GZIP))
            .from(Property.ofValue(uri.toString()))
            .build();

        var output = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of()));

        assertThat(output.getFiles().size(), is(1));
        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getFiles().get("folder/1.txt")), StandardCharsets.UTF_8)),
            is("1")
        );
    }

    @Test
    void stillRejectsInputThatIsNotGzip() throws Exception {
        var uri = compressUtils.uploadToStorageBytes(new byte[512]);

        var decompress = FileDecompress.builder()
            .id(IdUtils.create())
            .type(FileDecompress.class.getName())
            .compression(Property.ofValue(ArchiveDecompress.CompressionAlgorithm.GZIP))
            .from(Property.ofValue(uri.toString()))
            .build();

        assertThrows(IOException.class, () -> decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of())));
    }

    private String decompress(ArchiveDecompress.CompressionAlgorithm compression, byte[] content) throws Exception {
        var uri = compressUtils.uploadToStorageBytes(content);

        var decompress = FileDecompress.builder()
            .id(IdUtils.create())
            .type(FileDecompress.class.getName())
            .compression(Property.ofValue(compression))
            .from(Property.ofValue(uri.toString()))
            .build();

        var output = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of()));

        return CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()), StandardCharsets.UTF_8));
    }

    private static byte[] member(ArchiveDecompress.CompressionAlgorithm compression, String content) throws Exception {
        return member(compression, content.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] member(ArchiveDecompress.CompressionAlgorithm compression, byte[] content) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (OutputStream out = switch (compression) {
            case GZIP -> new GzipCompressorOutputStream(bytes);
            case BZIP2 -> new BZip2CompressorOutputStream(bytes);
            case XZ -> new XZCompressorOutputStream(bytes);
            case LZ4FRAME -> new FramedLZ4CompressorOutputStream(bytes);
            default -> throw new IllegalArgumentException(compression.name());
        }) {
            out.write(content);
        }
        return bytes.toByteArray();
    }
}
