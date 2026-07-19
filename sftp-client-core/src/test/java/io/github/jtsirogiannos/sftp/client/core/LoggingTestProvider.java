package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A provider registered through {@code META-INF/services} so the core's own tests can exercise
 * {@link SftpClients} end to end — discovery included — without dragging a real backend into this
 * module.
 */
public final class LoggingTestProvider implements SftpProvider {

    public static final String NAME = "logging-fake";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SftpCapabilities capabilities() {
        return SftpCapabilities.builder().maxSftpVersion(3).build();
    }

    @Override
    public SftpSession connect(SftpConnectionConfig config) {
        return new FakeSession();
    }

    static final class FakeSession implements SftpSession {

        private volatile boolean closed;

        @Override
        public void list(String path, java.util.function.Predicate<RemoteFile> visitor) {
            // no entries
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).build();
        }

        @Override
        public String realpath(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAttributes(String path,
                io.github.jtsirogiannos.sftp.client.api.AttributeUpdate update) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void hardlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.github.jtsirogiannos.sftp.client.api.FilesystemStats statvfs(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isValid() {
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public RemoteFile lstat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void symlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readlink(String linkPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteFile stat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mkdir(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(String from, String to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream openRead(String path, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream openWrite(String path, WriteMode mode) {
            throw new UnsupportedOperationException();
        }
    }
}
