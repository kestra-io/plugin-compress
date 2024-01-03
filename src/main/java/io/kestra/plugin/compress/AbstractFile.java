package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractFile extends AbstractTask {
    @Schema(
        title = "The algorithm compression of the archive file"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    protected CompressionAlgorithm compression;
}
