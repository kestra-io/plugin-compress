package io.kestra.plugin.compress;

import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;

import com.google.common.io.CharStreams;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
    @EnumSource(value = ArchiveDecompress.CompressionAlgorithm.class, names = {"GZIP", "BZIP2", "XZ"})
    void decompressesEveryMemberOfAConcatenatedFile(ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        // like `cat a.gz b.gz > both.gz`: two complete compressed members, one after the other
        var both = new ByteArrayOutputStream();
        both.writeBytes(member(compression, "first\n"));
        both.writeBytes(member(compression, "second\n"));
        URI uri = compressUtils.uploadToStorageBytes(both.toByteArray());

        FileDecompress decompress = FileDecompress.builder()
            .id("unit-test")
            .type(FileDecompress.class.getName())
            .compression(Property.ofValue(compression))
            .from(Property.ofValue(uri.toString()))
            .build();

        FileDecompress.Output output = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of()));

        assertThat(
            CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()), StandardCharsets.UTF_8)),
            is("first\nsecond\n")
        );
    }

    private static byte[] member(ArchiveDecompress.CompressionAlgorithm compression, String content) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (OutputStream out = switch (compression) {
            case GZIP -> new GzipCompressorOutputStream(bytes);
            case BZIP2 -> new BZip2CompressorOutputStream(bytes);
            case XZ -> new XZCompressorOutputStream(bytes);
            default -> throw new IllegalArgumentException(compression.name());
        }) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }
}
