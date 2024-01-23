package io.kestra.plugin.compress;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Compress an archive file."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "from:",
                "  myfile.txt: \"{{ inputs.files }} \"",
                "algorithm: ZIP",
                "compression: GZIP"
            }
        ),
        @Example(
            code = {
                "from: \"{{ outputs.taskId.uri }}\"",
                "algorithm: ZIP",
                "compression: GZIP"
            }
        )
    }
)
public class ArchiveCompress extends AbstractArchive implements RunnableTask<ArchiveCompress.Output> {
    @Schema(
        title = "The files to compress.",
        description = "The key must be a valid path in the archive and can contain `/` to represent the directory, " +
            "the value must be a Kestra internal storage URI.\n"+
            "The value can also be a JSON containing multiple keys/values."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    @NotNull
    private Object from;

    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            if (this.compression != null) {
                try (
                    CompressorOutputStream compressorOutputStream = this.compressorOutputStream(this.compression, outputStream);
                    ArchiveOutputStream archiveInputStream = this.archiveOutputStream(compressorOutputStream)
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

    @SuppressWarnings("unchecked")
    private void writeArchive(RunContext runContext, ArchiveOutputStream archiveInputStream) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> from = this.from instanceof String ?
            JacksonMapper.ofJson().readValue(
                runContext.render((String) this.from),
                new TypeReference<HashMap<String, String>>() {}
            ) :
            (Map<String, String>) this.from;

        for (Map.Entry<String, String> current : from.entrySet()) {
            // temp file and path
            String finalPath = runContext.render(current.getKey());
            File tempFile = runContext.resolve(Path.of(finalPath)).toFile();
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
            title = "URI of the compressed archive file on Kestra's internal storage."
        )
        private final URI uri;
    }
}
