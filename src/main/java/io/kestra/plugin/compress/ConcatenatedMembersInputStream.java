package io.kestra.plugin.compress;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.compress.compressors.CompressorInputStream;

// Reads every member of a concatenated GZIP, BZIP2 or LZ4 frame file (`cat a.gz b.gz > both.gz`).
// A new member only starts on the format's magic number, anything else after one good member (tar.gz zero padding) is ignored, as GNU gzip does.
class ConcatenatedMembersInputStream extends CompressorInputStream {
    @FunctionalInterface
    interface MemberOpener {
        InputStream open(InputStream in) throws IOException;
    }

    private final BufferedInputStream in;
    private final MemberOpener opener;
    private final byte[] magic;
    private final byte[] single = new byte[1];
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
        var n = read(single, 0, 1);
        return n == -1 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        while (member != null) {
            var n = member.read(b, off, len);
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
        var next = in.readNBytes(magic.length);
        in.reset();
        return Arrays.equals(next, magic);
    }

    @Override
    public void close() throws IOException {
        if (member != null) {
            member.close();
        }
        in.close();
    }
}
