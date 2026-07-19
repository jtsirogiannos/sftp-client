package io.github.jtsirogiannos.sftp.client.opentelemetry;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/** Counts what was actually read; an {@link InputStream} generally cannot be asked its size. */
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
