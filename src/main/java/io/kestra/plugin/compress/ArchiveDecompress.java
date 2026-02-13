package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    title = "Extract files from an archive",
    description = "Reads an archive from internal storage, optionally decompresses the stream (for example GZIP over TAR), and writes each entry back to storage with metrics for total size and file count. Entry names with spaces are stored using underscores."
)
@Plugin(
    metrics = {
        @Metric(name = "size", type = Counter.TYPE, description = "Total size of decompressed files in bytes"),
        @Metric(name = "count", type = Counter.TYPE, description = "Number of files decompressed")
    },
    examples = {
        @Example(
            full = true,
            code = """
                id: archive_decompress
                namespace: company.team

                inputs:
                  - id: file
                    description: Compressed file
                    type: FILE

                tasks:
                  - id: archive_decompress
                    type: io.kestra.plugin.compress.ArchiveDecompress
                    from: "{{ inputs.file }}"
                    algorithm: ZIP
                    compression: GZIP
                """
        )
    }
)
public class ArchiveDecompress extends AbstractArchive implements RunnableTask<ArchiveDecompress.Output> {
    @Schema(
        title = "Internal storage URI of the archive"
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    public Output run(RunContext runContext) throws Exception {
        Map<String, URI> files;

        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        try (
            InputStream fromInputStream = runContext.storage().getFile(from);
            InputStream fromInputStreamBuffered = new BufferedInputStream(fromInputStream);
        ) {
            if (this.compression != null) {
                try (
                    CompressorInputStream compressorInputStream = this.compressorInputStream(
                        runContext.render(this.compression).as(CompressionAlgorithm.class).orElseThrow(),
                        fromInputStreamBuffered
                    );
                    ArchiveInputStream archiveInputStream = this.archiveInputStream(compressorInputStream, runContext);
                ) {
                    files = this.readArchive(runContext, archiveInputStream);
                }
            } else {
                try (ArchiveInputStream archiveInputStream = this.archiveInputStream(fromInputStreamBuffered, runContext)) {
                    files = this.readArchive(runContext, archiveInputStream);
                }
            }
        }

        return Output.builder()
            .files(files)
            .build();
    }

    private Map<String, URI> readArchive(RunContext runContext, ArchiveInputStream archiveInputStream) throws IOException {
        HashMap<String, URI> files = new HashMap<>();

        long size = 0;
        ArchiveEntry entry;
        while ((entry = archiveInputStream.getNextEntry()) != null) {
            if (!archiveInputStream.canReadEntryData(entry)) {
                throw new IOException("Unable to read entry '" + entry.getName() + "'");
            }

            if (!entry.isDirectory()) {
                String sanitizedName = entry.getName().replaceAll(" ", "_");
                Path path = runContext.workingDir().createFile(sanitizedName);

                try (OutputStream o = Files.newOutputStream(path)) {
                    IOUtils.copy(archiveInputStream, o);
                }

                size = size + entry.getSize();
                files.put(entry.getName(), runContext.storage().putFile(path.toFile(), String.valueOf(path.getFileName())));
            }
        }

        runContext.metric(Counter.of("size", size));
        runContext.metric(Counter.of("count", files.size()));

        return files;
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URIs of decompressed files on Kestra's internal storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> files;
    }
}
