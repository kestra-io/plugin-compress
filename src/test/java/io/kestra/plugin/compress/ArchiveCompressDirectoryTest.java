package io.kestra.plugin.compress;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.Namespace;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ArchiveCompressDirectoryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private CompressUtils compressUtils;

    static Stream<Arguments> source() {
        return Stream.of(
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.ZIP, null),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.JAR, null),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, null),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.TAR, ArchiveDecompress.CompressionAlgorithm.GZIP),
            Arguments.of(ArchiveDecompress.ArchiveAlgorithm.CPIO, null)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void workingDirectory(ArchiveDecompress.ArchiveAlgorithm algorithm, ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        ArchiveCompress task = task(algorithm, compression).fromDirectory(Property.ofValue("data")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path().resolve("data");
        Files.createDirectories(root.resolve("sub/deeper"));
        Files.createDirectories(root.resolve("empty"));
        Files.writeString(root.resolve("a.txt"), "a");
        Files.writeString(root.resolve("sub/b.txt"), "b");
        Files.writeString(root.resolve("sub/deeper/c.txt"), "c");

        Map<String, String> files = read(task.run(runContext), algorithm, compression, runContext);

        assertThat(files.keySet(), containsInAnyOrder("a.txt", "sub/b.txt", "sub/deeper/c.txt"));
        assertThat(files.get("a.txt"), is("a"));
        assertThat(files.get("sub/b.txt"), is("b"));
        assertThat(files.get("sub/deeper/c.txt"), is("c"));
    }

    @Test
    void emptyDirectoriesAreArchived() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue("data")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path().resolve("data");
        Files.createDirectories(root.resolve("empty/nested"));
        Files.writeString(root.resolve("a.txt"), "a");

        Set<String> entries = entryNames(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, runContext);

        assertThat(entries, containsInAnyOrder("a.txt", "empty/", "empty/nested/"));
    }

    @Test
    void internalStorageDirectory() throws Exception {
        ArchiveCompress probe = task(ArchiveDecompress.ArchiveAlgorithm.TAR, null).fromDirectory(Property.ofValue("unused")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, probe, Map.of());

        URI uri = runContext.storage().putFile(new ByteArrayInputStream("s".getBytes(StandardCharsets.UTF_8)), "storage-test/dir/with%20space.txt");
        runContext.storage().putFile(new ByteArrayInputStream("n".getBytes(StandardCharsets.UTF_8)), "storage-test/dir/sub/n.txt");
        runContext.storage().putFile(new ByteArrayInputStream("d".getBytes(StandardCharsets.UTF_8)), "storage-test/dir/my%20sub/deeper%20dir/d.txt");

        // the directory URI is the parent of the file URI
        String directory = uri.toString().substring(0, uri.toString().lastIndexOf('/'));
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.TAR, null).fromDirectory(Property.ofValue(directory)).build();

        Map<String, String> files = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.TAR, null, runContext);

        assertThat(files.keySet(), containsInAnyOrder("with space.txt", "sub/n.txt", "my sub/deeper dir/d.txt"));
        assertThat(files.get("sub/n.txt"), is("n"));
        assertThat(files.get("my sub/deeper dir/d.txt"), is("d"));
    }

    @Test
    void internalStorageNamespaceLevelSkipsNamespaceMetadata() throws Exception {
        ArchiveCompress probe = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue("unused")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, probe, Map.of());

        String namespace = "kestra:///archive/test/" + IdUtils.create().toLowerCase();
        put(runContext, namespace + "/_files/ns.txt", "namespace file");
        put(runContext, namespace + "/_kv/key", "kv");
        put(runContext, namespace + "/my-flow/executions/exec1/tasks/t/run/out.txt", "out");
        put(runContext, namespace + "/my-flow/executions/exec1/tasks/t/run/_files/kept.txt", "kept");
        put(runContext, namespace + "/my-flow/executions/exec1/inputs/file/_data.csv", "input");
        put(runContext, namespace + "/my-flow/executions/exec1/trigger/t/_event.json", "trigger");

        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue(namespace)).build();

        Map<String, String> files = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, null, runContext);

        assertThat(
            files.keySet(), containsInAnyOrder(
                "my-flow/executions/exec1/tasks/t/run/out.txt",
                "my-flow/executions/exec1/tasks/t/run/_files/kept.txt",
                "my-flow/executions/exec1/inputs/file/_data.csv",
                "my-flow/executions/exec1/trigger/t/_event.json"
            )
        );
    }

    @Test
    void internalStorageFilesDirectoryOfAnExecutionIsArchivedAsIs() throws Exception {
        ArchiveCompress probe = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue("unused")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, probe, Map.of());

        String execution = "kestra:///archive/test/" + IdUtils.create().toLowerCase() + "/my-flow/executions/exec1";
        put(runContext, execution + "/tasks/t/run/_files/out/kept.txt", "kept");

        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null)
            .fromDirectory(Property.ofValue(execution + "/tasks/t/run/_files"))
            .build();

        Map<String, String> files = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, null, runContext);

        assertThat(files.keySet(), containsInAnyOrder("out/kept.txt"));
        assertThat(files.get("out/kept.txt"), is("kept"));
    }

    @Test
    void internalStorageNamespaceMetadataDirectoryIsRejected() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null)
            .fromDirectory(Property.ofValue("kestra:///archive/test/" + IdUtils.create().toLowerCase() + "/_kv"))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Exception exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("namespace metadata directory"));
    }

    @Test
    void fromAndFromDirectoryTogether() throws Exception {
        URI f1 = compressUtils.uploadToStorageString("map");
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null)
            .fromDirectory(Property.ofValue("data"))
            .from(Map.of("extra/map.txt", f1.toString()))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path().resolve("data");
        Files.createDirectories(root);
        Files.writeString(root.resolve("a.txt"), "a");

        Map<String, String> files = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, null, runContext);

        assertThat(files.keySet(), containsInAnyOrder("a.txt", "extra/map.txt"));
        assertThat(files.get("extra/map.txt"), is("map"));
    }

    @Test
    void workingDirectoryPathWithSpaces() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue("my data")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path().resolve("my data");
        Files.createDirectories(root);
        Files.writeString(root.resolve("a b.txt"), "a");

        Map<String, String> files = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, null, runContext);

        assertThat(files.keySet(), containsInAnyOrder("a b.txt"));
    }

    @Test
    void duplicateEntryBetweenFromAndFromDirectoryIsRejected() throws Exception {
        URI f1 = compressUtils.uploadToStorageString("map");
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.TAR, null)
            .fromDirectory(Property.ofValue("data"))
            .from(Map.of("a.txt", f1.toString()))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path().resolve("data");
        Files.createDirectories(root);
        Files.writeString(root.resolve("a.txt"), "a");

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Duplicate archive entry `a.txt`"));
    }

    @Test
    void emptyFromDirectoryIsRejected() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue(" ")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Exception exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("must not be empty"));
    }

    @Test
    void requiresFromOrFromDirectory() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Exception exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("`from` or `fromDirectory`"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "..", "../outside", "data/../..", "./.." })
    void directoryOutsideWorkingDirectoryIsRejected(String directory) throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue(directory)).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Files.createDirectories(runContext.workingDir().path().resolve("data"));

        Exception exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("must be a path inside the working directory"));
    }

    @Test
    void symbolicLinkOutsideWorkingDirectoryIsRejected() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue("link")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path outside = Files.createTempDirectory("outside");
        try {
            Files.createSymbolicLink(runContext.workingDir().path().resolve("link"), outside);
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.abort("symbolic links cannot be created here: " + e.getMessage());
        }

        Exception exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("must be a path inside the working directory"));
    }

    @ParameterizedTest
    @MethodSource("source")
    void wholeWorkingDirectoryDoesNotArchiveItself(ArchiveDecompress.ArchiveAlgorithm algorithm, ArchiveDecompress.CompressionAlgorithm compression) throws Exception {
        ArchiveCompress task = task(algorithm, compression).fromDirectory(Property.ofValue(".")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path();
        Files.createDirectories(root.resolve("sub"));
        Files.writeString(root.resolve("a.txt"), "a");
        Files.writeString(root.resolve("sub/b.txt"), "b");

        Map<String, String> files = read(task.run(runContext), algorithm, compression, runContext);

        assertThat(files.keySet(), containsInAnyOrder("a.txt", "sub/b.txt"));
    }

    @Test
    void arStoresNestedAndLongNames() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.AR, null).fromDirectory(Property.ofValue("data")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path root = runContext.workingDir().path().resolve("data");
        Files.createDirectories(root.resolve("2026/deep"));
        Files.writeString(root.resolve("a.txt"), "a");
        Files.writeString(root.resolve("a b.txt"), "space");
        Files.writeString(root.resolve("2026/deep/quarter-three.txt"), "q3");

        Map<String, String> files = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.AR, null, runContext);

        assertThat(files.keySet(), containsInAnyOrder("a.txt", "a b.txt", "2026/deep/quarter-three.txt"));
        assertThat(files.get("2026/deep/quarter-three.txt"), is("q3"));
        assertThat(files.get("a b.txt"), is("space"));
    }

    static Stream<Arguments> namespaceDirectories() {
        var id = IdUtils.create().toLowerCase();
        var withExecutionsSegment = "archive.test." + id + ".executions.x.y";
        return Stream.of(
            Arguments.of("archive.test." + id, "nsfile://archive.test." + id + "/reports"),
            Arguments.of("archive.test." + id + ".kestra", "kestra:///archive/test/" + id + "/kestra/_files/reports"),
            Arguments.of(withExecutionsSegment, "kestra:///archive/test/" + id + "/executions/x/y/_files/reports"),
            Arguments.of("archive.test." + id + ".executions.x.tasks.y", "kestra:///archive/test/" + id + "/executions/x/tasks/y/_files/reports"),
            Arguments.of(withExecutionsSegment + ".nsfile", "nsfile://" + withExecutionsSegment + ".nsfile/reports"),
            Arguments.of(withExecutionsSegment + ".uri", "kestra:///" + (withExecutionsSegment + ".uri").replace('.', '/') + "/_files/reports/")
        );
    }

    @ParameterizedTest
    @MethodSource("namespaceDirectories")
    void namespaceDirectoryArchivesLatestRevisionsOnly(String namespace, String directory) throws Exception {
        var task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue(directory)).build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var files = runContext.storage().namespace(namespace);
        putNamespaceFile(files, "reports/a.txt", "v1");
        putNamespaceFile(files, "reports/a.txt", "v2");
        putNamespaceFile(files, "reports/gone.txt", "gone");
        files.delete(Path.of("reports/gone.txt"));
        putNamespaceFile(files, "reports/sub/b.txt", "b");
        putNamespaceFile(files, "other/c.txt", "c");

        var archived = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, null, runContext);

        assertThat(archived.keySet(), containsInAnyOrder("a.txt", "sub/b.txt"));
        assertThat(archived.get("a.txt"), is("v2"));
    }

    @Test
    void namespaceLevelSkipsFilesOfChildNamespaceWithExecutionsSegment() throws Exception {
        var parent = "archive.test." + IdUtils.create().toLowerCase();
        var task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null)
            .fromDirectory(Property.ofValue("kestra:///" + parent.replace('.', '/')))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        putNamespaceFile(runContext.storage().namespace(parent + ".executions.x.y"), "reports/a.txt", "namespace file");
        put(runContext, "kestra:///" + parent.replace('.', '/') + "/my-flow/executions/exec1/tasks/t/run/out.txt", "out");

        var archived = read(task.run(runContext), ArchiveDecompress.ArchiveAlgorithm.ZIP, null, runContext);

        assertThat(archived.keySet(), containsInAnyOrder("my-flow/executions/exec1/tasks/t/run/out.txt"));
    }

    @Test
    void missingNamespaceDirectoryIsRejected() throws Exception {
        var namespace = "archive.test." + IdUtils.create().toLowerCase();
        var task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null)
            .fromDirectory(Property.ofValue("nsfile://" + namespace + "/nope"))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        putNamespaceFile(runContext.storage().namespace(namespace), "reports/a.txt", "a");

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Directory not found in the namespace"));
    }

    @Test
    void unsupportedSchemeIsRejected() throws Exception {
        ArchiveCompress task = task(ArchiveDecompress.ArchiveAlgorithm.ZIP, null).fromDirectory(Property.ofValue("http://example.com/dir")).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Exception exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Scheme not supported"));
    }

    private static ArchiveCompress.ArchiveCompressBuilder<?, ?> task(ArchiveDecompress.ArchiveAlgorithm algorithm, ArchiveDecompress.CompressionAlgorithm compression) {
        return ArchiveCompress.builder()
            .id(IdUtils.create())
            .type(ArchiveCompress.class.getName())
            .algorithm(Property.ofValue(algorithm))
            .compression(compression == null ? null : Property.ofValue(compression));
    }

    private static void put(RunContext runContext, String uri, String content) throws IOException {
        runContext.storage().putFile(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), URI.create(uri));
    }

    private static void putNamespaceFile(Namespace namespace, String path, String content) throws Exception {
        namespace.putFile(Path.of(path), new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), Namespace.Conflicts.OVERWRITE);
    }

    /** Decompresses the archive with the plugin's own decompressor and returns the file contents by entry name. */
    private Map<String, String> read(ArchiveCompress.Output output, ArchiveDecompress.ArchiveAlgorithm algorithm, ArchiveDecompress.CompressionAlgorithm compression, RunContext runContext)
        throws Exception {
        ArchiveDecompress decompress = ArchiveDecompress.builder()
            .id(IdUtils.create())
            .type(ArchiveDecompress.class.getName())
            .algorithm(Property.ofValue(algorithm))
            .compression(compression == null ? null : Property.ofValue(compression))
            .from(Property.ofValue(output.getUri().toString()))
            .build();

        ArchiveDecompress.Output decompressed = decompress.run(TestsUtils.mockRunContext(runContextFactory, decompress, Map.of()));

        Map<String, String> files = new HashMap<>();
        for (Map.Entry<String, URI> file : decompressed.getFiles().entrySet()) {
            try (InputStream in = runContext.storage().getFile(file.getValue())) {
                files.put(file.getKey(), new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        return files;
    }

    private Set<String> entryNames(ArchiveCompress.Output output, ArchiveDecompress.ArchiveAlgorithm algorithm, RunContext runContext) throws Exception {
        ArchiveCompress reader = task(algorithm, null).fromDirectory(Property.ofValue("unused")).build();
        Set<String> names = new java.util.HashSet<>();
        try (
            InputStream in = runContext.storage().getFile(output.getUri());
            ArchiveInputStream<?> archive = reader.archiveInputStream(in, runContext)
        ) {
            ArchiveEntry entry;
            while ((entry = archive.getNextEntry()) != null) {
                names.add(entry.getName());
                IOUtils.consume(archive);
            }
        }
        return names;
    }
}
