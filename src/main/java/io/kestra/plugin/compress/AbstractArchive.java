package io.kestra.plugin.compress;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
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
import io.kestra.core.models.annotations.PluginProperty;

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
    @PluginProperty(group = "main")
    protected Property<ArchiveAlgorithm> algorithm;

    @Schema(
        title = "Optional compressor applied to the archive stream",
        description = "Use a single-file compressor such as GZIP alongside TAR. Leave null to store the archive uncompressed. Brotli, Deflate64, and Snappy variants are decode-only and will fail during compression."
    )
    protected Property<ArchiveDecompress.CompressionAlgorithm> compression;

    protected ArchiveInputStream archiveInputStream(InputStream inputStream, RunContext runContext) throws ArchiveException, IllegalVariableEvaluationException, IOException {
        ArchiveAlgorithm algorithm = runContext.render(this.algorithm).as(ArchiveAlgorithm.class)
            .orElseThrow(() -> new IllegalArgumentException("Unknown algorithm"));

        // ArchiveStreamFactory.detect() needs to peek at the stream signature, so mark support is mandatory
        InputStream detectableInputStream = inputStream.markSupported() ? inputStream : new BufferedInputStream(inputStream);
        checkArchiveFormat(detectableInputStream, algorithm);

        return switch (algorithm) {
            case AR -> new ArArchiveInputStream(detectableInputStream);
            case ARJ -> new ArjArchiveInputStream(detectableInputStream);
            case CPIO -> new CpioArchiveInputStream(detectableInputStream);
            case DUMP -> new DumpArchiveInputStream(detectableInputStream);
            case JAR -> new JarArchiveInputStream(detectableInputStream);
            case TAR -> new TarArchiveInputStream(detectableInputStream);
            case ZIP -> new ZipArchiveInputStream(detectableInputStream);
        };

    }

    /**
     * Fails fast when the archive content does not match the declared algorithm.
     * <p>
     * Most readers already fail on a mismatch, but the TAR one silently yields no entry at all, which makes the task
     * succeed with an empty output. When the format cannot be detected at all the check is skipped so the reader keeps
     * the last word: an empty archive, for instance, carries no detectable signature.
     */
    private void checkArchiveFormat(InputStream inputStream, ArchiveAlgorithm algorithm) throws IOException {
        String detected;
        try {
            detected = ArchiveStreamFactory.detect(inputStream);
        } catch (ArchiveException | IllegalArgumentException e) {
            return;
        }

        if (!matches(algorithm, detected)) {
            throw new IllegalArgumentException(
                "The archive is not in the '" + algorithm + "' format, detected format is '" +
                    detected.toUpperCase(Locale.ROOT) + "'."
            );
        }
    }

    private static boolean matches(ArchiveAlgorithm algorithm, String detected) {
        // a JAR is a ZIP, both are reported as ZIP by the detection
        if (algorithm == ArchiveAlgorithm.JAR) {
            return ArchiveStreamFactory.JAR.equals(detected) || ArchiveStreamFactory.ZIP.equals(detected);
        }

        return algorithm.name().equalsIgnoreCase(detected);
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
