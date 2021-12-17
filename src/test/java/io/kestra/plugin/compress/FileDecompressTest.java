package io.kestra.plugin.compress;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
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
            .compression(compression)
            .from(uri.toString())
            .build();

        FileDecompress.Output runDecompress = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, ImmutableMap.of()));

        assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(runDecompress.getUri()))), is("1"));
    }
}
