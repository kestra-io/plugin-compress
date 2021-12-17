package io.kestra.plugin.compress;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.stream.Stream;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class FileTest {
    @Inject
    private CompressUtils compressUtils;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.BZIP2),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.DEFLATE),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.GZIP),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.LZ4BLOCK),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.LZ4FRAME),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.LZMA),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.SNAPPYFRAME),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.XZ),
            Arguments.of(ArchiveDecompress.CompressionAlgorithm.ZSTD)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void suite(ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        URI f1 = compressUtils.uploadToStorageString("1");

        FileCompress compress = FileCompress.builder()
            .id("unit-test")
            .type(ArchiveCompress.class.getName())
            .compression(compression)
            .from(f1.toString())
            .build();

        FileCompress.Output runCompress = compress.run(TestsUtils.mockRunContext(runContextFactory, compress, ImmutableMap.of()));

        FileDecompress decompress = FileDecompress.builder()
            .id("unit-test")
            .type(ArchiveDecompress.class.getName())
            .compression(compression)
            .from(runCompress.getUri().toString())
            .build();

        FileDecompress.Output runDecompress = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, ImmutableMap.of()));

        assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(runDecompress.getUri()))), is("1"));
    }
}
