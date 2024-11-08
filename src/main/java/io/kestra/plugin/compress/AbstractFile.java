package io.kestra.plugin.compress;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractFile extends AbstractTask {
    @Schema(
        title = "The algorithm compression of the archive file"
    )
    @NotNull
    protected Property<CompressionAlgorithm> compression;
}
