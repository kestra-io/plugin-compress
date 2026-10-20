package io.kestra.plugin.compress;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.io.IOUtils;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.namespaces.files.NamespaceFileMetadata;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.property.URIFetcher;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.core.storages.StorageContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.scheduler.Schedulers;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an archive from multiple files or a directory",
    description = "Builds an archive from rendered file map inputs stored in internal storage and/or from a whole directory walked recursively with relative paths preserved, optionally wrapping it with a stream compressor (for example TAR + GZIP). Fails for algorithms that are extract-only."
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
        ),
        @Example(
            full = true,
            title = "Compress a whole namespace directory, preserving relative paths",
            code = """
                id: archive_compress_directory
                namespace: company.team

                tasks:
                  - id: archive_compress
                    type: "io.kestra.plugin.compress.ArchiveCompress"
                    fromDirectory: "nsfile:///reports"
                    algorithm: ZIP
                """
        ),
        @Example(
            full = true,
            title = "Compress a directory of the task working directory",
            code = """
                id: archive_compress_working_directory
                namespace: company.team

                tasks:
                  - id: working_dir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: generate
                        type: io.kestra.plugin.scripts.shell.Commands
                        taskRunner:
                          type: io.kestra.plugin.core.runner.Process
                        commands:
                          - mkdir -p data/nested && echo hello > data/nested/a.txt

                      - id: archive_compress
                        type: "io.kestra.plugin.compress.ArchiveCompress"
                        fromDirectory: "data"
                        algorithm: TAR
                        compression: GZIP
                """
        )
    }
)
public class ArchiveCompress extends AbstractArchive implements RunnableTask<ArchiveCompress.Output>, Data.From {
    private static final Pattern SCHEME_PREFIX = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+://");
    // only the layout StorageContext writes (tasks/<task>/<taskRun>, trigger/<trigger>, inputs/<input>), so that a namespace like `a.b.executions.c.d` or `a.b.executions.c.tasks.d` is not taken for an execution
    private static final Pattern EXECUTION_PATH = Pattern
        .compile("^/(?:[^/_][^/]*/){2,}executions/[^/_][^/]*(?:/tasks(?:/[^/_][^/]*(?:/[^/_][^/]*(?:/.*)?)?)?|/(?:trigger|inputs)(?:/[^/_][^/]*(?:/.*)?)?)?/?$");
    // /my/namespace/_files/some/dir: the internal storage location of the files of the namespace my.namespace
    private static final Pattern NAMESPACE_FILES_PATH = Pattern.compile("^/((?:[^/_][^/]*/)*[^/_][^/]*)/_files(?:/(.*))?$");

    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION + " Optional when `fromDirectory` is set."
    )
    @PluginProperty(group = "main")
    private Object from;

    @Schema(
        title = "Directory to archive recursively",
        description = """
            Every file below this directory is added to the archive with its path relative to the directory root, and directories (including empty ones) are added as entries, except for the AR format which has no directory support. Supported locations:
            - a namespace directory, e.g. `nsfile:///path/to/dir` (current namespace) or `nsfile://my.namespace/path/to/dir`;
            - a namespace directory given as its internal storage URI, e.g. `kestra:///my/namespace/_files/path/to/dir`, handled like `nsfile://`;
            - any other directory of Kestra's internal storage, e.g. a `kestra:///...` execution output directory, whose stored files are archived as they are; namespace metadata directories (`_files`, `_kv`, ...) met at the namespace level are skipped;
            - a path inside the task working directory, e.g. `my/dir` or `.` (symbolic links are skipped, the archive being created is never included).
            The directory must exist.
            Can be combined with `from`: the directory content is written first, then the `from` entries, and a `from` entry named like a file of the directory is rejected."""
    )
    @PluginProperty(group = "main")
    private Property<String> fromDirectory;

    public Output run(RunContext runContext) throws Exception {
        if (this.from == null && this.fromDirectory == null) {
            throw new IllegalArgumentException("At least one of `from` or `fromDirectory` must be set");
        }

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
                    this.writeArchive(runContext, archiveInputStream, tempFile.toPath());
                }
            } else {
                try (ArchiveOutputStream archiveOutputStream = this.archiveOutputStream(outputStream, runContext)) {
                    this.writeArchive(runContext, archiveOutputStream, tempFile.toPath());
                }
            }
        }

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @SuppressWarnings("unchecked")
    private void writeArchive(RunContext runContext, ArchiveOutputStream archiveInputStream, Path output) throws Exception {
        var writtenNames = new HashSet<String>();

        if (this.fromDirectory != null) {
            var directory = runContext.render(this.fromDirectory).as(String.class).filter(d -> !d.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("`fromDirectory` must not be empty"));
            writtenNames.addAll(this.writeDirectory(runContext, archiveInputStream, directory, output));
        }

        if (this.from != null) {
            Data.from(this.from)
                .read(runContext)
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(throwConsumer(map ->
                {
                    for (Map.Entry<String, Object> current : map.entrySet()) {

                        // temp file and path
                        String finalPath = runContext.render(current.getKey());
                        if (writtenNames.contains(finalPath)) {
                            throw new IllegalArgumentException("Duplicate archive entry `" + finalPath + "`: it is already added by `fromDirectory`");
                        }
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
        }

        archiveInputStream.finish();
    }

    // a file's content is either already in the working directory (localPath) or in internal storage (storageUri)
    private record DirectoryItem(String name, boolean directory, Path localPath, URI storageUri) {
    }

    private Set<String> writeDirectory(RunContext runContext, ArchiveOutputStream archive, String directory, Path output) throws Exception {
        var directoriesSupported = runContext.render(this.algorithm).as(ArchiveAlgorithm.class).orElseThrow() != ArchiveAlgorithm.AR;

        var items = this.listDirectory(runContext, directory, output);
        items.sort(Comparator.comparing(DirectoryItem::name));
        var names = new HashSet<String>();

        for (var item : items) {
            if (item.directory()) {
                if (directoriesSupported) {
                    // the entry factories read the type from a real directory, whose name does not matter
                    var tempDir = Files.createTempDirectory(runContext.workingDir().path(), "dir");
                    try {
                        archive.putArchiveEntry(archive.createArchiveEntry(tempDir.toFile(), item.name() + "/"));
                        archive.closeArchiveEntry();
                    } finally {
                        Files.deleteIfExists(tempDir);
                    }
                }
                continue;
            }

            names.add(item.name());
            var file = item.localPath();
            var temporary = file == null;
            if (temporary) {
                file = runContext.workingDir().createTempFile();
                try (var in = runContext.storage().getFile(item.storageUri())) {
                    Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING);
                }
            }

            try {
                archive.putArchiveEntry(archive.createArchiveEntry(file.toFile(), item.name()));
                try (var in = Files.newInputStream(file)) {
                    IOUtils.copy(in, archive);
                }
                archive.closeArchiveEntry();
            } finally {
                if (temporary) {
                    Files.deleteIfExists(file);
                }
            }
        }

        return names;
    }

    private List<DirectoryItem> listDirectory(RunContext runContext, String directory, Path output) throws Exception {
        // only a `scheme://` value is a URI: anything else is a working directory path, which may hold spaces or backslashes
        URI uri = SCHEME_PREFIX.matcher(directory).find() ? URI.create(directory) : null;
        var scheme = uri == null ? null : uri.getScheme();

        if (Namespace.NAMESPACE_FILE_SCHEME.equals(scheme)) {
            return this.listNamespaceDirectory(runContext, uri.getAuthority(), uri.getPath());
        } else if (StorageContext.KESTRA_SCHEME.equals(scheme)) {
            var path = uri.getPath() == null || uri.getPath().isEmpty() ? "/" : uri.getPath();

            // a `_files` directory inside an execution is plain execution output, not namespace files
            if (!EXECUTION_PATH.matcher(path).matches()) {
                // namespace files are versioned: their raw storage holds old revisions and deleted files
                var namespaceFiles = NAMESPACE_FILES_PATH.matcher(path);
                if (namespaceFiles.matches()) {
                    return this.listNamespaceDirectory(runContext, namespaceFiles.group(1).replace('/', '.'), "/" + Objects.toString(namespaceFiles.group(2), ""));
                }
                if (path.contains("/" + StorageContext.RESERVED_NAMESPACE_DIR_PREFIX)) {
                    throw new IllegalArgumentException("`fromDirectory` cannot archive a namespace metadata directory: " + directory);
                }
            }

            var items = new ArrayList<DirectoryItem>();
            this.listStorageDirectory(runContext, uri, "", items);
            return items;
        } else if (scheme != null) {
            throw new IllegalArgumentException("Scheme not supported: " + scheme + ". `fromDirectory` supports `nsfile://`, `kestra://` and working directory paths");
        }

        return this.listWorkingDirectory(runContext, directory, output);
    }

    private List<DirectoryItem> listNamespaceDirectory(RunContext runContext, String namespaceName, String path) throws Exception {
        var namespace = namespaceName == null ? runContext.storage().namespace() : runContext.storage().namespace(namespaceName);
        // normalised like children() does with its argument, so that the paths it returns start with the prefix
        var root = NamespaceFile.toLogicalPath(NamespaceFile.normalize(Path.of(path == null || path.isEmpty() ? "/" : path)));
        var prefix = root.endsWith("/") ? root : root + "/";

        if (!"/".equals(prefix) && !isNamespaceDirectory(namespace, prefix)) {
            throw new IllegalArgumentException("Directory not found in the namespace `" + namespace.namespace() + "`: " + root);
        }

        var items = new ArrayList<DirectoryItem>();
        for (var child : namespace.children(prefix, true)) {
            var childPath = child.getPath();
            if (!childPath.startsWith(prefix) || childPath.length() == prefix.length()) {
                continue;
            }

            if (child.isDirectory()) {
                items.add(new DirectoryItem(childPath.substring(prefix.length(), childPath.length() - 1), true, null, null));
            } else {
                items.add(new DirectoryItem(childPath.substring(prefix.length()), false, null, namespace.get(Path.of(childPath)).uri()));
            }
        }

        return items;
    }

    // namespace directories are metadata entries whose path ends with `/`, listed among the children of their parent
    private static boolean isNamespaceDirectory(Namespace namespace, String prefix) throws IOException {
        return namespace.children(NamespaceFileMetadata.parentPath(prefix), false).stream()
            .anyMatch(child -> child.isDirectory() && child.getPath().equals(prefix));
    }

    private void listStorageDirectory(RunContext runContext, URI directory, String relative, List<DirectoryItem> items) throws Exception {
        // the raw path keeps names escaped: decoding it would make the child URIs invalid for names like `my sub`
        var base = directory.getRawPath().endsWith("/") ? directory.getRawPath() : directory.getRawPath() + "/";

        for (var attributes : runContext.storage().list(directory)) {
            var name = relative + attributes.getFileName();
            // encoded as an absolute path, so that a name like `a:b` is not read as a scheme
            var child = URI.create(StorageContext.KESTRA_PROTOCOL + base + new URI(null, null, "/" + attributes.getFileName(), null).getRawPath().substring(1));

            // namespace metadata directories (`_files`, `_kv`, ...) are versioned, outside executions they are not plain files
            if (attributes.getFileName().startsWith(StorageContext.RESERVED_NAMESPACE_DIR_PREFIX) && !EXECUTION_PATH.matcher(child.getPath()).matches()) {
                continue;
            }

            if (attributes.getType() == FileAttributes.FileType.Directory) {
                items.add(new DirectoryItem(name, true, null, null));
                this.listStorageDirectory(runContext, URI.create(child + "/"), name + "/", items);
            } else {
                items.add(new DirectoryItem(name, false, null, child));
            }
        }
    }

    private List<DirectoryItem> listWorkingDirectory(RunContext runContext, String directory, Path output) throws IOException {
        var workingDir = runContext.workingDir().path().toRealPath();
        // normalised then resolved to its real path, so that neither `..` nor a symbolic link can lead outside the working directory
        var root = workingDir.resolve(directory).normalize();
        if (!root.startsWith(workingDir)) {
            throw new IllegalArgumentException("`fromDirectory` must be a path inside the working directory: " + directory);
        }
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Directory not found in the working directory: " + directory);
        }
        root = root.toRealPath();
        if (!root.startsWith(workingDir)) {
            throw new IllegalArgumentException("`fromDirectory` must be a path inside the working directory: " + directory);
        }

        // the archive is being written in the working directory, it must not archive itself (e.g. with `fromDirectory: "."`)
        var excluded = output.toRealPath();

        var items = new ArrayList<DirectoryItem>();
        try (var walk = Files.walk(root)) {
            for (var path : (Iterable<Path>) walk::iterator) {
                if (path.equals(root) || path.equals(excluded)) {
                    continue;
                }

                var name = root.relativize(path).toString().replace(File.separatorChar, '/');
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    items.add(new DirectoryItem(name, true, null, null));
                } else if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    items.add(new DirectoryItem(name, false, path, null));
                }
            }
        }

        return items;
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
