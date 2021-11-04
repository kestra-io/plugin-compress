package io.kestra.plugin.compress;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunnerUtils;
import io.kestra.runner.memory.MemoryRunner;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.Matchers.is;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * This test will load all flow located in `src/test/resources/flows/`
 * and will run an in-memory runner to be able to test a full flow. There is also a
 * configuration file in `src/test/resources/application.yml` that is only for the full runner
 * test to configure in-memory runner.
 */
@MicronautTest
class ArchiveRunnerTest {
    @Inject
    protected MemoryRunner runner;

    @Inject
    protected RunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private CompressUtils compressUtils;

    @Inject
    private StorageInterface storageInterface;

    @BeforeEach
    private void init() throws IOException, URISyntaxException {
        if (!this.runner.isRunning()) {
            repositoryLoader.load(Objects.requireNonNull(ArchiveRunnerTest.class.getClassLoader().getResource("flows")));
            this.runner.run();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void flowString() throws Exception {
        URI f1 = compressUtils.uploadToStorageString("1");
        URI f2 = compressUtils.uploadToStorageString("2");
        URI f3 = compressUtils.uploadToStorageString("3");


        Map<String, URI> inputsContent = ImmutableMap.of("file1.txt", f1, "file2.txt", f2, "file3.txt", f3);

        Map < String, String > inputs = Map.of(
            "json", JacksonMapper.ofJson().writeValueAsString(inputsContent)
        );

        Execution execution = runnerUtils.runOne(
            "io.kestra.plugin-compress",
            "archiveCompressString",
            null,
            (f, e) -> runnerUtils.typedInputs(f, e, inputs)
        );

        Map<String, String> outputs = (Map<String, String>) execution.getTaskRunList().get(1).getOutputs().get("files");

        assertThat(outputs.size(),is(3));

        outputs.entrySet().forEach(throwConsumer( stringStringEntry ->
            assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(new URI(stringStringEntry.getValue())))),
            is(CharStreams.toString(new InputStreamReader(storageInterface.get(inputsContent.get(stringStringEntry.getKey()))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    void flowMap() throws Exception {
        URI f1 = compressUtils.uploadToStorageString("1");
        URI f2 = compressUtils.uploadToStorageString("2");
        URI f3 = compressUtils.uploadToStorageString("3");


        Map<String, String> inputs = Map.of(
            "file1", f1.toString(),
            "file2", f2.toString(),
            "file3", f3.toString()
        );

        Execution execution = runnerUtils.runOne(
            "io.kestra.plugin-compress",
            "archiveCompressMap",
            null,
            (f, e) -> runnerUtils.typedInputs(f, e, inputs)
        );

        Map<String, String> outputs = (Map<String, String>) execution.getTaskRunList().get(1).getOutputs().get("files");

        Map<String, URI> inputsContent = ImmutableMap.of("f1.txt", f1, "f2.txt", f2, "f3.txt", f3);

        outputs.entrySet().forEach(throwConsumer( stringStringEntry ->
            assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(new URI(stringStringEntry.getValue())))),
                is(CharStreams.toString(new InputStreamReader(storageInterface.get(inputsContent.get(stringStringEntry.getKey()))))))));
    }
}