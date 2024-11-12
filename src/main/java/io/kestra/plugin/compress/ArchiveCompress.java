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
import jakarta.validation.constraints.NotNull;
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
            full = true,
            code = """
                id: archive_compress
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: "archive_compress"
                    type: "io.kestra.plugin.compress.ArchiveCompress"
                    from:
                      myfile.txt: "{{ inputs.file }}"
                    algorithm: ZIP
                """
        ),
        @Example(
            full = true,
            code = """
                id: archive_compress
                namespace: company.team

                tasks:
                  - id: products_download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://raw.githubusercontent.com/kestra-io/datasets/main/csv/products.csv"

                  - id: orders_download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://raw.githubusercontent.com/kestra-io/datasets/main/csv/orders.csv"
                
                  - id: archive_compress
                    type: "io.kestra.plugin.compress.ArchiveCompress"
                    from:
                      products.csv: "{{ outputs.products_download.uri }}"
                      orders.csv: "{{ outputs.orders_download.uri }}"
                    algorithm: TAR
                    compression: GZIP
                """
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
        File tempFile = runContext.workingDir().createTempFile().toFile();

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            if (this.compression != null) {
                try (
                    CompressorOutputStream compressorOutputStream = this.compressorOutputStream(
                        runContext.render(this.compression).as(CompressionAlgorithm.class).orElseThrow(),
                        outputStream
                    );
                    ArchiveOutputStream archiveInputStream = this.archiveOutputStream(compressorOutputStream, runContext)
                ) {
                    this.writeArchive(runContext, archiveInputStream);
                }
            } else {
                try (ArchiveOutputStream archiveOutputStream = this.archiveOutputStream(outputStream, runContext)) {
                    this.writeArchive(runContext, archiveOutputStream);
                }
            }
        }

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
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
            File tempFile = runContext.workingDir().resolve(Path.of(finalPath)).toFile();
            new File(tempFile.getParent()).mkdirs();

            // write to temp file
            String render = runContext.render(current.getValue());
            OutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
            InputStream inputStream = runContext.storage().getFile(URI.create(render));

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
