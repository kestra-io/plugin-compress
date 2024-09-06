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

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import jakarta.validation.constraints.NotNull;

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
            full = true,
            code = """
                id: file_decompress
                namespace: company.team
                
                inputs:
                  - id: file  
                    description: File to be decompressed
                    type: FILE
                
                tasks:
                  - id: decompress
                    type: io.kestra.plugin.compress.FileDecompress
                    from: "{{ inputs.file }}"
                    compression: Z
                """
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
        Path tempFile = runContext.workingDir().createTempFile();

        try (
            OutputStream outputStream = Files.newOutputStream(tempFile);
            InputStream inputStream = runContext.storage().getFile(URI.create(runContext.render(this.from)));
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
            .uri(runContext.storage().putFile(tempFile.toFile()))
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
