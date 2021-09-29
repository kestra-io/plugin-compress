package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.compressors.CompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Compress a file"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "from: \"{{ inputs.files }} \"",
                "compression: Z"
            }
        )
    }
)
public class FileCompress extends AbstractTask implements RunnableTask<FileCompress.Output> {
    @NotNull
    @Schema(
        title = "The file internal storage uri"
    )
    @PluginProperty(dynamic = true)
    private String from;

    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            try (
                CompressorOutputStream compressorOutputStream = this.compressorOutputStream(outputStream);
                InputStream inputStream = runContext.uriToInputStream(URI.create(runContext.render(this.from)))
            ) {
                final byte[] buffer = new byte[8192];
                int n = 0;
                while (-1 != (n = inputStream.read(buffer))) {
                    compressorOutputStream.write(buffer, 0, n);
                }
            }

        }

        return Output.builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The url of the zip file on kestra storage"
        )
        private final URI uri;
    }
}
