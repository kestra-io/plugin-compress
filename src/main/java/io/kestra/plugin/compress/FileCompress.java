package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.compressors.CompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Compress a file."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: file_compress
                namespace: company.team

                inputs:
                  - id: file
                    description: File to be compressed
                    type: FILE

                tasks:
                  - id: compress
                    type: io.kestra.plugin.compress.FileCompress
                    from: "{{ inputs.file }}"
                    compression: Z
                """
        )
    }
)
public class FileCompress extends AbstractFile implements RunnableTask<FileCompress.Output> {
    @Schema(
        title = "The file's internal storage URI."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            try (
                CompressorOutputStream compressorOutputStream = this.compressorOutputStream(
                    runContext.render(this.compression).as(CompressionAlgorithm.class).orElseThrow(),
                    outputStream
                );
                InputStream inputStream = runContext.storage().getFile(URI.create(runContext.render(this.from).as(String.class).orElseThrow()))
            ) {
                final byte[] buffer = new byte[8192];
                int n = 0;
                while (-1 != (n = inputStream.read(buffer))) {
                    compressorOutputStream.write(buffer, 0, n);
                }
            }

        }

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the compressed file on Kestra's internal storage."
        )
        private final URI uri;
    }
}
