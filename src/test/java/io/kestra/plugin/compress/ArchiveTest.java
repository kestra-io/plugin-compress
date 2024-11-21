package io.kestra.plugin.compress;

import com.google.common.io.CharStreams;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ArchiveTest {
    @Inject
    private CompressUtils compressUtils;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    //Return pair of <Archive algorithm, CompressionAlgorithm>
    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.BZIP2),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.DEFLATE),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.GZIP),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.LZ4BLOCK),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.LZ4FRAME),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.LZMA),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.SNAPPYFRAME),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.XZ),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.ZSTD),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.ZIP, null)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void suite(ArchiveDecompress.ArchiveAlgorithm format, ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        URI f1 = compressUtils.uploadToStorageString("1");
        URI f2 = compressUtils.uploadToStorageString("2");
        URI f3 = compressUtils.uploadToStorageString("3");

        ArchiveCompress compress = ArchiveCompress.builder()
            .id("unit-test")
            .type(ArchiveCompress.class.getName())
            .algorithm(Property.of(format))
            .compression(compression == null ? null : Property.of(compression))
            .from(Map.of(
                "folder/subfolder/1.txt", f1.toString(),
                "folder/2.txt", f2.toString(),
                "3.txt", f3.toString()
            ))
            .build();

        ArchiveCompress.Output runCompress = compress.run(TestsUtils.mockRunContext(runContextFactory, compress, Map.of()));

        ArchiveDecompress decompress = ArchiveDecompress.builder()
            .id("unit-test")
            .type(ArchiveDecompress.class.getName())
            .algorithm(Property.of(format))
            .compression(compression == null ? null : Property.of(compression))
            .from(Property.of(runCompress.getUri().toString()))
            .build();

        ArchiveDecompress.Output runDecompress = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of()));

        MatcherAssert.assertThat(runDecompress.getFiles().size(), is(3));assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(null, null, runDecompress.getFiles().get("folder/subfolder/1.txt")))), is("1"));
        assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(null, null, runDecompress.getFiles().get("folder/2.txt")))), is("2"));
        assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(null, null, runDecompress.getFiles().get("3.txt")))), is("3"));
    }
}
