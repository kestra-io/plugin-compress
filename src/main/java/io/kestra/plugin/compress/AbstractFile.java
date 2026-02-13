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
        title = "Compressor applied to the single file",
        description = "Required compression algorithm for a single file. Brotli, Deflate64, and Snappy variants are decode-only and cannot be used when writing."
    )
    @NotNull
    protected Property<CompressionAlgorithm> compression;
}
