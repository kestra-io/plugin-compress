package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
    title = "Decompress an archive file."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "from: \"{{ inputs.files }}\"",
                "algorithm: ZIP",
                "compression: GZIP"
            }
        )
    }
)
public class ArchiveDecompress extends AbstractArchive implements RunnableTask<ArchiveDecompress.Output> {
    @NotNull
    @Schema(
        title = "The file's internal storage URI."
    )
    @PluginProperty(dynamic = true)
    private String from;

    public Output run(RunContext runContext) throws Exception {
        Map<String, URI> files;

        URI from = new URI(runContext.render(this.from));
        try (
            InputStream fromInputStream = runContext.storage().getFile(from);
            InputStream fromInputStreamBuffered = new BufferedInputStream(fromInputStream);
        ) {
            if (this.compression != null) {
                try (
                    CompressorInputStream compressorInputStream = this.compressorInputStream(this.compression, fromInputStreamBuffered);
                    ArchiveInputStream archiveInputStream = this.archiveInputStream(compressorInputStream);
                ) {
                    files = this.readArchive(runContext, archiveInputStream);
                }
            } else {
                try (ArchiveInputStream archiveInputStream = this.archiveInputStream(fromInputStreamBuffered)) {
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
                Path path = runContext.workingDir().createFile(entry.getName());
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
            title = "URI of the decompressed archive file on Kestra's internal storage."
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> files;
    }
}
