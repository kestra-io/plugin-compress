package io.kestra.plugin.compress;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {
    @Test
    @ExecuteFlow("sanity-checks/archive.yaml")
    void archive(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(7));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/file_encrypt_decrypt.yaml")
    void fileEncryptDecrypt(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(10));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
