package io.kestra.plugin.compress;

import com.google.common.io.CharStreams;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowInputOutput;
import io.kestra.core.runners.TestRunner;
import io.kestra.core.runners.TestRunnerUtils;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * This test will load all flow located in `src/test/resources/flows/`
 * and will run an in-memory runner to be able to test a full flow. There is also a
 * configuration file in `src/test/resources/application.yml` that is only for the full runner
 * test to configure in-memory runner.
 */
@KestraTest
class ArchiveRunnerTest {
    @Inject
    protected TestRunner runner;

    @Inject
    protected TestRunnerUtils testRunnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private CompressUtils compressUtils;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private FlowInputOutput flowIO;

    @BeforeEach
    protected void init() throws IOException, URISyntaxException {
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

        Map<String, URI> inputsContent = Map.of("file1.txt", f1, "file2.txt", f2, "file3.txt", f3);

        Map<String, Object> inputs = Map.of(
            "json", JacksonMapper.ofJson().writeValueAsString(inputsContent)
        );

        Execution execution = testRunnerUtils.runOne(
            TenantService.MAIN_TENANT,
            "io.kestra.plugin-compress",
            "archiveCompressString",
            null,
            (f, e) -> flowIO.readExecutionInputs(f, e, inputs)
        );

        Map<String, String> outputs = (Map<String, String>) execution.getTaskRunList().get(1).getOutputs().get("files");

        assertThat(outputs.size(),is(3));

        outputs.entrySet().forEach(throwConsumer( stringStringEntry ->
            assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, new URI(stringStringEntry.getValue())))),
            is(CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, inputsContent.get(stringStringEntry.getKey()))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    void flowMap() throws Exception {
        URI f1 = compressUtils.uploadToStorageString("1");
        URI f2 = compressUtils.uploadToStorageString("2");
        URI f3 = compressUtils.uploadToStorageString("3");


        Map<String, Object> inputs = Map.of(
            "file1", f1.toString(),
            "file2", f2.toString(),
            "file3", f3.toString()
        );

        Execution execution = testRunnerUtils.runOne(
            TenantService.MAIN_TENANT,
            "io.kestra.plugin-compress",
            "archiveCompressMap",
            null,
            (f, e) -> flowIO.readExecutionInputs(f, e, inputs)
        );

        Map<String, String> outputs = (Map<String, String>) execution.getTaskRunList().get(1).getOutputs().get("files");

        Map<String, URI> inputsContent = Map.of("f1.txt", f1, "f2.txt", f2, "f3.txt", f3);

        outputs.entrySet().forEach(throwConsumer( stringStringEntry ->
            assertThat(CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, new URI(stringStringEntry.getValue())))),
                is(CharStreams.toString(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, inputsContent.get(stringStringEntry.getKey()))))))));
    }
}
