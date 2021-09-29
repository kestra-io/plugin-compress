package io.kestra.plugin.compress;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Decompress an archive file"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "from:",
                "  myfile.txt: \"{{ inputs.files }} \"",
                "algorithm: ZIP"
            }
        )
    }
)
public class ArchiveCompress extends AbstractArchive implements RunnableTask<ArchiveCompress.Output> {
    @NotNull
    @Schema(
        title = "The file internal storage uri"
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> from;

    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            if (this.compression != null) {
                try (
                    CompressorOutputStream compressorOutputStream = this.compressorOutputStream(outputStream);
                    ArchiveOutputStream archiveInputStream = this.archiveOutputStream(compressorOutputStream);
                ) {
                    this.writeArchive(runContext, archiveInputStream);
                }
            } else {
                try (ArchiveOutputStream archiveOutputStream = this.archiveOutputStream(outputStream)) {
                    this.writeArchive(runContext, archiveOutputStream);
                }
            }
        }

        return Output.builder()
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    private void writeArchive(RunContext runContext, ArchiveOutputStream archiveInputStream) throws IOException, IllegalVariableEvaluationException {
        Path tempDir = runContext.tempDir();

        for (Map.Entry<String, String> current: this.from.entrySet()) {
            // temp file and path
            String finalPath = runContext.render(current.getKey());
            File tempFile = tempDir.resolve(finalPath).toFile();
            new File(tempFile.getParent()).mkdirs();

            // write to temp file
            String render = runContext.render(current.getValue());
            OutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
            InputStream inputStream = runContext.uriToInputStream(URI.create(render));

            IOUtils.copy(inputStream, fileOutputStream);
            fileOutputStream.flush();

            // create archive entry
            ArchiveEntry entry = archiveInputStream.createArchiveEntry(tempFile, finalPath);
            archiveInputStream.putArchiveEntry(entry);

            // write archive entry
            try (InputStream i = Files.newInputStream(tempFile.toPath())) {
                IOUtils.copy(i, archiveInputStream);
            }
            archiveInputStream.closeArchiveEntry();
        }

        archiveInputStream.finish();
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
