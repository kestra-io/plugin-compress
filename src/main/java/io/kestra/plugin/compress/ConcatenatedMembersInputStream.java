package io.kestra.plugin.compress;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.compress.compressors.CompressorInputStream;

/**
 * Reads every member of a concatenated GZIP, BZIP2 or LZ4 frame file, like `cat a.gz b.gz > both.gz`.
 * <p>
 * Each member is read with a single-member stream. After a member ends, the next one is only started
 * when the following bytes carry the format's magic number. Anything else after at least one good member
 * (for example the zero padding of a tar.gz) is ignored, the same way GNU gzip does. The first member is
 * always opened, so input that is not in the format still fails.
 */
class ConcatenatedMembersInputStream extends CompressorInputStream {
    @FunctionalInterface
    interface MemberOpener {
        InputStream open(InputStream in) throws IOException;
    }

    private final BufferedInputStream in;
    private final MemberOpener opener;
    private final byte[] magic;
    private InputStream member;

    ConcatenatedMembersInputStream(InputStream inputStream, byte[] magic, MemberOpener opener) throws IOException {
        // single-member streams leave the underlying stream right after their member when it supports mark/reset
        this.in = new BufferedInputStream(inputStream);
        this.opener = opener;
        this.magic = magic.clone();
        this.member = opener.open(this.in);
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n == -1 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        while (member != null) {
            int n = member.read(b, off, len);
            if (n >= 0) {
                count(n);
                return n;
            }
            member = nextMemberFollows() ? opener.open(in) : null;
        }
        return -1;
    }

    private boolean nextMemberFollows() throws IOException {
        in.mark(magic.length);
        byte[] next = in.readNBytes(magic.length);
        in.reset();
        return Arrays.equals(next, magic);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
