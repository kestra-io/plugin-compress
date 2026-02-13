package io.kestra.plugin.compress;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ar.ArArchiveInputStream;
import org.apache.commons.compress.archivers.ar.ArArchiveOutputStream;
import org.apache.commons.compress.archivers.arj.ArjArchiveInputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveInputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.apache.commons.compress.archivers.dump.DumpArchiveInputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveInputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import java.io.InputStream;
import java.io.OutputStream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractArchive extends AbstractTask {
    @Schema(
        title = "Archive container format to create",
        description = "Required archive format. Compression supports AR, CPIO, JAR, TAR, and ZIP; ARJ and DUMP are extract-only and will fail on compression."
    )
    @NotNull
    protected Property<ArchiveAlgorithm> algorithm;

    @Schema(
        title = "Optional compressor applied to the archive stream",
        description = "Use a single-file compressor such as GZIP alongside TAR. Leave null to store the archive uncompressed. Brotli, Deflate64, and Snappy variants are decode-only and will fail during compression."
    )
    protected Property<ArchiveDecompress.CompressionAlgorithm> compression;

    protected ArchiveInputStream archiveInputStream(InputStream inputStream, RunContext runContext) throws ArchiveException, IllegalVariableEvaluationException {
        var renderedAlgorithm = runContext.render(this.algorithm).as(ArchiveAlgorithm.class);
        return switch (renderedAlgorithm.orElseThrow(() -> new IllegalArgumentException("Unknown algorithm"))) {
            case AR -> new ArArchiveInputStream(inputStream);
            case ARJ -> new ArjArchiveInputStream(inputStream);
            case CPIO -> new CpioArchiveInputStream(inputStream);
            case DUMP -> new DumpArchiveInputStream(inputStream);
            case JAR -> new JarArchiveInputStream(inputStream);
            case TAR -> new TarArchiveInputStream(inputStream);
            case ZIP -> new ZipArchiveInputStream(inputStream);
        };

    }

    protected ArchiveOutputStream archiveOutputStream(OutputStream outputStream, RunContext runContext) throws ArchiveException, IllegalVariableEvaluationException {
        var renderedAlgorithm = runContext.render(this.algorithm).as(ArchiveAlgorithm.class);
        return switch (renderedAlgorithm.orElseThrow(() -> new IllegalArgumentException("Unknown algorithm"))) {
            case AR -> new ArArchiveOutputStream(outputStream);
            case CPIO -> new CpioArchiveOutputStream(outputStream);
            case JAR -> new JarArchiveOutputStream(outputStream);
            case TAR -> {
                TarArchiveOutputStream out = new TarArchiveOutputStream(outputStream);
                out.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX); // allow long file name
                out.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX); // allow large archive name

                yield out;
            }
            case ZIP -> new ZipArchiveOutputStream(outputStream);
            default -> throw new IllegalArgumentException("Unknown algorithm '" + renderedAlgorithm.get() + "'");
        };

    }

    public enum ArchiveAlgorithm {
        AR,
        ARJ,
        CPIO,
        DUMP,
        JAR,
        TAR,
        ZIP
    }
}
