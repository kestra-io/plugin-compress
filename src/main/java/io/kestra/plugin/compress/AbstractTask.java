package io.kestra.plugin.compress;

import io.kestra.core.models.tasks.Task;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.deflate64.Deflate64CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractTask extends Task {
    protected CompressorInputStream compressorInputStream(CompressionAlgorithm compression, InputStream inputStream) throws IOException {
        switch (compression) {
            case BROTLI:
                return new BrotliCompressorInputStream(inputStream);
            case BZIP2:
                return new BZip2CompressorInputStream(inputStream);
            case DEFLATE:
                return new DeflateCompressorInputStream(inputStream);
            case DEFLATE64:
                return new Deflate64CompressorInputStream(inputStream);
            case GZIP:
                return new GzipCompressorInputStream(inputStream);
            case LZ4BLOCK:
                return new BlockLZ4CompressorInputStream(inputStream);
            case LZ4FRAME:
                return new FramedLZ4CompressorInputStream(inputStream);
            case LZMA:
                return new LZMACompressorInputStream(inputStream);
            case SNAPPY:
                return new SnappyCompressorInputStream(inputStream);
            case SNAPPYFRAME:
                return new FramedSnappyCompressorInputStream(inputStream);
            case XZ:
                return new XZCompressorInputStream(inputStream);
            case ZSTD:
                return new ZstdCompressorInputStream(inputStream);
            case Z:
                return new ZCompressorInputStream(inputStream);
        }

        throw new IllegalArgumentException("Unknown compression '" + compression + "'");
    }

    protected CompressorOutputStream compressorOutputStream(CompressionAlgorithm compression, OutputStream outputStream) throws IOException {
        switch (compression) {
            case BROTLI:
            case DEFLATE64:
            case SNAPPY:
                throw new IllegalArgumentException("Not implemented compression '" + compression + "'");
                 // return new SnappyCompressorOutputStream(outputStream, uncompressedSize);
            case BZIP2:
                return new BZip2CompressorOutputStream(outputStream);
            case DEFLATE:
                return new DeflateCompressorOutputStream(outputStream);
            case GZIP:
                return new GzipCompressorOutputStream(outputStream);
            case LZ4BLOCK:
                return new BlockLZ4CompressorOutputStream(outputStream);
            case LZ4FRAME:
                return new FramedLZ4CompressorOutputStream(outputStream);
            case LZMA:
                return new LZMACompressorOutputStream(outputStream);
            case SNAPPYFRAME:
                return new FramedSnappyCompressorOutputStream(outputStream);
            case XZ:
                return new XZCompressorOutputStream(outputStream);
            case ZSTD:
                return new ZstdCompressorOutputStream(outputStream);
        }

        throw new IllegalArgumentException("Unknown compression '" + compression + "'");
    }

    public enum CompressionAlgorithm {
        BROTLI,
        BZIP2,
        DEFLATE,
        DEFLATE64,
        GZIP,
        LZ4BLOCK,
        LZ4FRAME,
        LZMA,
        SNAPPY,
        SNAPPYFRAME,
        XZ,
        Z,
        ZSTD
    }
}
