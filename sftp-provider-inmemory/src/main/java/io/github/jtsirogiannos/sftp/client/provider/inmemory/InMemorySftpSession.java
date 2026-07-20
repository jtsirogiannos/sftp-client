package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.provider.inmemory.InMemoryFileSystem.Node;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A session over {@link InMemoryFileSystem}. No sockets, no threads, no crypto.
 *
 * <p>Implements {@link AsyncSftpSession} as well as the blocking contract, which is not a pretence:
 * with no wire to wait on, every operation really does complete without parking a thread. The two are
 * sibling interfaces, so implementing both is a claim about this backend rather than an inheritance
 * artefact — and it is what keeps {@link io.github.jtsirogiannos.sftp.client.core}'s async client
 * live, exercised code rather than a path waiting for a provider that might never be written.
 */
final class InMemorySftpSession implements SftpSession, AsyncSftpSession {

    /** Chunk size for readAsync. Arbitrary: there is no round trip to amortise, only allocation. */
    private static final int READ_CHUNK = 32 * 1024;

    private final InMemoryFileSystem filesystem;
    private final SftpCapabilities capabilities;
    private volatile boolean closed;

    InMemorySftpSession(InMemoryFileSystem filesystem, SftpCapabilities capabilities) {
        this.filesystem = filesystem;
        this.capabilities = capabilities;
    }

    @Override
    public void list(String path, Predicate<RemoteFile> visitor) {
        // The whole listing is taken under the lock, then visited outside it: a visitor that stops
        // early must not be able to hold the filesystem, and one that calls back in must not
        // deadlock. Dot entries are never produced, so there is nothing to filter.
        List<String> children = filesystem.read(() -> filesystem.childPaths(path));
        for (String child : children) {
            RemoteFile entry = filesystem.read(() -> toRemoteFile(child, filesystem.lstat(child)));
            if (!visitor.test(entry)) {
                return;
            }
        }
    }

    @Override
    public RemoteFile stat(String path) {
        return filesystem.read(() -> toRemoteFile(path, filesystem.stat(path)));
    }

    @Override
    public RemoteFile lstat(String path) {
        return filesystem.read(() -> toRemoteFile(path, filesystem.lstat(path)));
    }

    @Override
    public boolean exists(String path) {
        return filesystem.read(() -> filesystem.exists(path));
    }

    @Override
    public String realpath(String path) {
        return filesystem.read(() -> InMemoryFileSystem.normalise(path));
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        requireCapability(capabilities.symlinks(), "symbolic links");
        filesystem.read(() -> {
            filesystem.put(linkPath, Node.symlink(targetPath));
            return null;
        });
    }

    @Override
    public String readlink(String linkPath) {
        requireCapability(capabilities.symlinks(), "symbolic links");
        return filesystem.read(() -> {
            Node node = filesystem.lstat(linkPath);
            if (node.kind != Node.Kind.SYMLINK) {
                throw new SftpException("not a symlink: " + linkPath, null,
                        SftpException.SSH_FX_FAILURE);
            }
            return node.linkTarget;
        });
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        requireCapability(capabilities.hardLinks(), "hard links");
        filesystem.read(() -> {
            filesystem.hardlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        if (update.isEmpty()) {
            return;
        }
        filesystem.read(() -> {
            Node node = filesystem.stat(path);
            update.permissions().ifPresent(mode -> node.permissions = mode);
            update.modifiedTime().ifPresent(when -> node.modified = when);
            update.ownerId().ifPresent(uid -> node.ownerId = uid);
            update.groupId().ifPresent(gid -> node.groupId = gid);
            // Setting the size is the protocol's only truncate, and it can extend too.
            update.size().ifPresent(size -> {
                if (node.kind != Node.Kind.FILE) {
                    throw new SftpException("not a regular file: " + path, null,
                            SftpException.SSH_FX_FAILURE);
                }
                byte[] resized = new byte[(int) size];
                System.arraycopy(node.content, 0, resized, 0,
                        Math.min(node.content.length, resized.length));
                node.content = resized;
            });
            return null;
        });
    }

    @Override
    public FilesystemStats statvfs(String path) {
        requireCapability(capabilities.statvfs(), "statvfs");
        return filesystem.read(() -> {
            filesystem.stat(path); // fail the same way a real server would for a missing path
            return new FilesystemStats(4096, InMemoryFileSystem.TOTAL_BYTES,
                    InMemoryFileSystem.FREE_BYTES, InMemoryFileSystem.FREE_BYTES / 2,
                    1_000_000, 900_000);
        });
    }

    @Override
    public void mkdir(String path) {
        filesystem.read(() -> {
            filesystem.createDirectory(path);
            return null;
        });
    }

    @Override
    public void rename(String from, String to) {
        filesystem.read(() -> {
            // Replacement only where the capability claims it; a session reporting v3 semantics has
            // to behave like one, or the fake teaches its users the wrong lesson.
            filesystem.rename(from, to, capabilities.posixRename());
            return null;
        });
    }

    @Override
    public void remove(String path) {
        filesystem.read(() -> {
            filesystem.remove(path);
            return null;
        });
    }

    @Override
    public InputStream openRead(String path, long offset) {
        return filesystem.read(() -> {
            Node node = filesystem.stat(path);
            if (node.kind == Node.Kind.DIRECTORY) {
                throw new SftpException("is a directory: " + path, null,
                        SftpException.SSH_FX_FAILURE);
            }
            byte[] content = node.content;
            if (offset >= content.length) {
                return new ByteArrayInputStream(new byte[0]); // past EOF is empty, not an error
            }
            return new ByteArrayInputStream(content, (int) offset, (int) (content.length - offset));
        });
    }

    @Override
    public OutputStream openWrite(String path, WriteMode mode) {
        return filesystem.read(() -> {
            String resolved = InMemoryFileSystem.normalise(path);
            boolean present = filesystem.exists(resolved);
            if (mode == WriteMode.CREATE_NEW) {
                requireCapability(capabilities.atomicCreateNew(), "exclusive create");
                if (present) {
                    throw new SftpException("already exists: " + path, null,
                            SftpException.SSH_FX_FILE_ALREADY_EXISTS);
                }
            }
            byte[] existing = present && mode == WriteMode.APPEND
                    ? filesystem.lstat(resolved).content
                    : new byte[0];
            // Buffered until close, which is when the write becomes visible — the contract says an
            // upload is not durable until the stream is closed.
            return new NodeOutputStream(resolved, existing);
        });
    }

    // --- the async half ---
    //
    // Every one of these completes inline: there is no I/O to wait for, so a completed stage is the
    // truthful answer rather than a shortcut. What they exercise is the core's dispatch, not latency.

    @Override
    public CompletionStage<List<RemoteFile>> listAsync(String path) {
        return complete(() -> list(path));
    }

    @Override
    public CompletionStage<RemoteFile> statAsync(String path) {
        return complete(() -> stat(path));
    }

    @Override
    public CompletionStage<Boolean> existsAsync(String path) {
        return complete(() -> exists(path));
    }

    @Override
    public CompletionStage<Void> mkdirAsync(String path) {
        return complete(() -> {
            mkdir(path);
            return null;
        });
    }

    @Override
    public CompletionStage<Void> renameAsync(String from, String to) {
        return complete(() -> {
            rename(from, to);
            return null;
        });
    }

    @Override
    public CompletionStage<Void> removeAsync(String path) {
        return complete(() -> {
            remove(path);
            return null;
        });
    }

    @Override
    public CompletionStage<RemoteFile> lstatAsync(String path) {
        return complete(() -> lstat(path));
    }

    @Override
    public CompletionStage<String> realpathAsync(String path) {
        return complete(() -> realpath(path));
    }

    @Override
    public CompletionStage<Void> symlinkAsync(String linkPath, String targetPath) {
        return complete(() -> {
            symlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public CompletionStage<String> readlinkAsync(String linkPath) {
        return complete(() -> readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlinkAsync(String linkPath, String targetPath) {
        return complete(() -> {
            hardlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public CompletionStage<Void> setAttributesAsync(String path, AttributeUpdate update) {
        return complete(() -> {
            setAttributes(path, update);
            return null;
        });
    }

    @Override
    public CompletionStage<FilesystemStats> statvfsAsync(String path) {
        return complete(() -> statvfs(path));
    }

    @Override
    public Flow.Publisher<ByteBuffer> readAsync(String path, long offset) {
        byte[] content;
        try {
            content = filesystem.read(() -> {
                Node node = filesystem.stat(path);
                if (node.kind == Node.Kind.DIRECTORY) {
                    throw new SftpException("is a directory: " + path, null,
                            SftpException.SSH_FX_FAILURE);
                }
                return node.content;
            });
        } catch (RuntimeException e) {
            // The failure belongs to the subscriber, not to whoever called readAsync — a publisher
            // that throws on construction is not something a subscriber can observe or recover from.
            return subscriber -> {
                subscriber.onSubscribe(new InertSubscription());
                subscriber.onError(e);
            };
        }
        byte[] window = offset >= content.length
                ? new byte[0]   // past EOF is an empty stream, not an error
                : Arrays.copyOfRange(content, (int) offset, content.length);
        return new ByteArrayPublisher(window, READ_CHUNK);
    }

    @Override
    public CompletionStage<Void> writeAsync(String path, Flow.Publisher<ByteBuffer> data,
            WriteMode mode) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        OutputStream out;
        try {
            out = openWrite(path, mode);
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
            return result;
        }
        data.subscribe(new CollectingSubscriber(out, result));
        return result;
    }

    @Override
    public SftpCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public boolean isValid() {
        return !closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    /** Runs {@code work} now, turning a thrown failure into a failed stage rather than a throw. */
    private static <T> CompletionStage<T> complete(Supplier<T> work) {
        try {
            return CompletableFuture.completedFuture(work.get());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /** For a publisher that is already terminal before demand can mean anything. */
    private static final class InertSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    }

    /**
     * Drains a publisher into the open stream one buffer at a time.
     *
     * <p>{@code request(1)} per element rather than unbounded: the write is the slow end here in
     * spirit even if not in fact, and a subscriber that asks for everything up front is one that
     * buffers the whole file in memory — the exact thing the streaming API exists to avoid.
     */
    private static final class CollectingSubscriber implements Flow.Subscriber<ByteBuffer> {

        private final OutputStream out;
        private final CompletableFuture<Void> result;
        private Flow.Subscription subscription;

        CollectingSubscriber(OutputStream out, CompletableFuture<Void> result) {
            this.out = out;
            this.result = result;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(ByteBuffer item) {
            try {
                byte[] chunk = new byte[item.remaining()];
                item.get(chunk);
                out.write(chunk);
                subscription.request(1);
            } catch (IOException e) {
                subscription.cancel();
                closeQuietly();
                result.completeExceptionally(
                        new SftpException("upload failed", e));
            }
        }

        @Override
        public void onError(Throwable throwable) {
            closeQuietly();
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            // Closing is what publishes the write, so a failure here fails the upload — reporting
            // success before the bytes are visible would be the one lie this contract cannot tell.
            try {
                out.close();
                result.complete(null);
            } catch (IOException | RuntimeException e) {
                result.completeExceptionally(new SftpException("upload failed on close", e));
            }
        }

        private void closeQuietly() {
            try {
                out.close();
            } catch (IOException ignored) {
                // Already failing; the original cause is the interesting one.
            }
        }
    }

    private static void requireCapability(boolean present, String what) {
        if (!present) {
            throw new UnsupportedOperationException(
                    "this session does not support " + what + "; check capabilities() first");
        }
    }

    private RemoteFile toRemoteFile(String path, Node node) {
        String normalised = InMemoryFileSystem.normalise(path);
        String name = normalised.substring(normalised.lastIndexOf('/') + 1);
        return new RemoteFile(name.isEmpty() ? "/" : name, normalised, node.size(),
                node.kind == Node.Kind.DIRECTORY, node.kind == Node.Kind.SYMLINK,
                node.modified, node.permissions, node.ownerId, node.groupId);
    }

    /** Collects the write and publishes it to the filesystem on close. */
    private final class NodeOutputStream extends ByteArrayOutputStream {

        private final String path;
        private boolean published;

        NodeOutputStream(String path, byte[] existing) {
            this.path = path;
            writeBytes(existing);
        }

        @Override
        public void close() {
            if (published) {
                return;
            }
            published = true;
            filesystem.read(() -> {
                filesystem.put(path, Node.file(toByteArray()));
                return null;
            });
        }
    }
}
