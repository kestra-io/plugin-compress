package io.kestra.plugin.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

import io.kestra.core.models.tasks.Task;

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
public abstract class AbstractTask extends Task {
    private static final byte[] GZIP_MAGIC = {0x1f, (byte) 0x8b};
    private static final byte[] BZIP2_MAGIC = {'B', 'Z', 'h'};
    private static final byte[] LZ4_FRAME_MAGIC = {0x04, 0x22, 0x4d, 0x18};

    protected CompressorInputStream compressorInputStream(CompressionAlgorithm compression, InputStream inputStream) throws IOException {
        return switch (compression) {
            case BROTLI -> new BrotliCompressorInputStream(inputStream);
            case BZIP2 -> new ConcatenatedMembersInputStream(inputStream, BZIP2_MAGIC, in -> new BZip2CompressorInputStream(in, false));
            case DEFLATE -> new DeflateCompressorInputStream(inputStream);
            case DEFLATE64 -> new Deflate64CompressorInputStream(inputStream);
            case GZIP -> new ConcatenatedMembersInputStream(inputStream, GZIP_MAGIC, in -> GzipCompressorInputStream.builder().setInputStream(in).get());
            case LZ4BLOCK -> new BlockLZ4CompressorInputStream(inputStream);
            case LZ4FRAME -> new ConcatenatedMembersInputStream(inputStream, LZ4_FRAME_MAGIC, in -> new FramedLZ4CompressorInputStream(in, false));
            case LZMA -> new LZMACompressorInputStream(inputStream);
            case SNAPPY -> new SnappyCompressorInputStream(inputStream);
            case SNAPPYFRAME -> new FramedSnappyCompressorInputStream(inputStream);
            // XZ reads concatenated streams and the stream padding allowed by the format by itself
            case XZ -> XZCompressorInputStream.builder().setInputStream(inputStream).setDecompressConcatenated(true).get();
            case ZSTD -> new ZstdCompressorInputStream(inputStream);
            case Z -> new ZCompressorInputStream(inputStream);
        };

    }

    protected CompressorOutputStream compressorOutputStream(CompressionAlgorithm compression, OutputStream outputStream) throws IOException {
        return switch (compression) {
            case BROTLI, DEFLATE64, SNAPPY ->
                throw new IllegalArgumentException("Not implemented compression '" + compression + "'");
            case BZIP2 -> new BZip2CompressorOutputStream(outputStream);
            case DEFLATE -> new DeflateCompressorOutputStream(outputStream);
            case GZIP -> new GzipCompressorOutputStream(outputStream);
            case LZ4BLOCK -> new BlockLZ4CompressorOutputStream(outputStream);
            case LZ4FRAME -> new FramedLZ4CompressorOutputStream(outputStream);
            case LZMA -> new LZMACompressorOutputStream(outputStream);
            case SNAPPYFRAME -> new FramedSnappyCompressorOutputStream(outputStream);
            case XZ -> new XZCompressorOutputStream(outputStream);
            case ZSTD -> new ZstdCompressorOutputStream(outputStream);
            default -> throw new IllegalArgumentException("Unknown compression '" + compression + "'");
        };

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
