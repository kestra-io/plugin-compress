package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Decompress a file."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "from: \"{{ inputs.files }}\"",
                "compression: Z"
            }
        )
    }
)
public class FileDecompress extends AbstractFile implements RunnableTask<FileDecompress.Output> {
    @NotNull
    @Schema(
        title = "The file's internal storage URI."
    )
    @PluginProperty(dynamic = true)
    private String from;

    public Output run(RunContext runContext) throws Exception {
        Path tempFile = runContext.tempFile();

        try (
            OutputStream outputStream = Files.newOutputStream(tempFile);
            InputStream inputStream = runContext.uriToInputStream(URI.create(runContext.render(this.from)));
            InputStream inputStreamBuffer = new BufferedInputStream(inputStream);
            CompressorInputStream compressorInputStream = this.compressorInputStream(this.compression, inputStreamBuffer);
        ) {
            final byte[] buffer = new byte[8192];
            int n = 0;
            while (-1 != (n = compressorInputStream.read(buffer))) {
                outputStream.write(buffer, 0, n);
            }
        }

        return Output.builder()
            .uri(runContext.putTempFile(tempFile.toFile()))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the decompressed file on Kestra's internal storage."
        )
        private final URI uri;
    }
}
