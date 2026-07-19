package io.github.jtsirogiannos.sftp.client.micrometer;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Counts what was actually read, which is the only honest measure of an upload's size.
 *
 * <p>The alternative — asking the source how big it is — assumes the source knows, and an
 * {@code InputStream} generally does not. Counting as it drains also means a transfer that fails
 * half way reports the half that moved rather than nothing or everything.
 */
final class CountingInputStream extends FilterInputStream {

    private long count;

    CountingInputStream(InputStream in) {
        super(in);
    }

    long count() {
        return count;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b != -1) {
            count++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = super.read(b, off, len);
        if (read > 0) {
            count += read;
        }
        return read;
    }
}
