package io.kestra.plugin.compress;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
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
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractArchive extends AbstractTask {
    @Schema(
        title = "The algorithm of the archive file"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    protected ArchiveAlgorithm algorithm;

    @Schema(
        title = "The compression used for the archive file. Some algorithms focus on compressing individual files (for example GZIP), while others compress and combine multiple files into a single archive. The single-file compressor is often used alongside a separate tool for archiving multiple files (TAR and GZIP for example)"
    )
    @PluginProperty(dynamic = false)
    protected ArchiveDecompress.CompressionAlgorithm compression;

    protected ArchiveInputStream archiveInputStream(InputStream inputStream) throws ArchiveException {
        switch (this.algorithm) {
            case AR:
                return new ArArchiveInputStream(inputStream);
            case ARJ:
                return new ArjArchiveInputStream(inputStream);
            case CPIO:
                return new CpioArchiveInputStream(inputStream);
            case DUMP:
                return new DumpArchiveInputStream(inputStream);
            case JAR:
                return new JarArchiveInputStream(inputStream);
            case TAR:
                return new TarArchiveInputStream(inputStream);
            case ZIP:
                return new ZipArchiveInputStream(inputStream);
        }

        throw new IllegalArgumentException("Unknown algorithm '" + this.algorithm + "'");
    }

    protected ArchiveOutputStream archiveOutputStream(OutputStream outputStream) throws ArchiveException {
        switch (this.algorithm) {
            case AR:
                return new ArArchiveOutputStream(outputStream);
            case CPIO:
                return new CpioArchiveOutputStream(outputStream);
            case JAR:
                return new JarArchiveOutputStream(outputStream);
            case TAR:
                return new TarArchiveOutputStream(outputStream);
            case ZIP:
                return new ZipArchiveOutputStream(outputStream);
        }

        throw new IllegalArgumentException("Unknown algorithm '" + this.algorithm + "'");
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
