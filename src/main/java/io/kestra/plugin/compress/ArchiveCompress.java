package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.URIFetcher;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.io.IOUtils;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an archive from multiple files",
    description = "Builds an archive from rendered file map inputs stored in internal storage, optionally wrapping it with a stream compressor (for example TAR + GZIP). Fails for algorithms that are extract-only."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Compress an input file",
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
            title = "Download two files, compress them together and upload to S3 bucket",
            code = """
                id: archive_compress
                namespace: company.team

                tasks:
                  - id: products_download
                    type: io.kestra.plugin.core.http.Download
                    uri: "http://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv"

                  - id: orders_download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv"

                  - id: archive_compress
                    type: "io.kestra.plugin.compress.ArchiveCompress"
                    from:
                      products.csv: "{{ outputs.products_download.uri }}"
                      orders.csv: "{{ outputs.orders_download.uri }}"
                    algorithm: TAR
                    compression: GZIP

                  - id: upload_compressed
                    type: io.kestra.plugin.aws.s3.Upload
                    bucket: "example"
                    region: "{{ secret('AWS_REGION') }}"
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    from: "{{ outputs.archive_compress.uri }}"
                    key: "archive.gz"
                """
        )
    }
)
public class ArchiveCompress extends AbstractArchive implements RunnableTask<ArchiveCompress.Output>, Data.From {
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION
    )
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
    private void writeArchive(RunContext runContext, ArchiveOutputStream archiveInputStream) throws Exception {
        Data.from(this.from)
            .read(runContext)
            .publishOn(Schedulers.boundedElastic())
            .doOnNext(throwConsumer(map -> {
                for (Map.Entry<String, Object> current : map.entrySet()) {

                    // temp file and path
                    String finalPath = runContext.render(current.getKey());
                    File tempFile = runContext.workingDir().resolve(Path.of(finalPath)).toFile();
                    new File(tempFile.getParent()).mkdirs();

                    // write to temp file
                    String render = runContext.render(current.getValue().toString());
                    OutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
                    InputStream inputStream = URIFetcher.of(URI.create(render)).fetch(runContext);

                    IOUtils.copy(inputStream, fileOutputStream);
                    fileOutputStream.flush();
                    fileOutputStream.close();

                    // create archive entry
                    ArchiveEntry entry = archiveInputStream.createArchiveEntry(tempFile, finalPath);
                    archiveInputStream.putArchiveEntry(entry);

                    // write archive entry
                    try (InputStream i = Files.newInputStream(tempFile.toPath())) {
                        IOUtils.copy(i, archiveInputStream);
                    }
                    archiveInputStream.closeArchiveEntry();
                }
            }))
            .blockLast();

        archiveInputStream.finish();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the compressed archive file on Kestra's internal storage"
        )
        private final URI uri;
    }
}
