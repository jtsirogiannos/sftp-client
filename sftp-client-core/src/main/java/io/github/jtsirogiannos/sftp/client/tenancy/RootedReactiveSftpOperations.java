package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpPaths;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * The reactive operations, confined to a {@link RootDirectory} — {@link RootedSftpOperations}, for
 * the reactive facade, with the same rules and the same refusals.
 *
 * <p>A refusal is a failed stage, never a throw: a caller composing stages cannot catch a synchronous
 * exception, which is the reactive facade's own rule. A refused {@code download} is a publisher that
 * fails its subscriber.
 */
final class RootedReactiveSftpOperations implements ReactiveSftpOperations {

    private final ReactiveSftpOperations delegate;
    private final RootDirectory root;

    RootedReactiveSftpOperations(ReactiveSftpOperations delegate, RootDirectory root) {
        this.delegate = delegate;
        this.root = root;
    }

    /** A session confined the same way, whose lifecycle stays with {@code session}. */
    static ReactiveSftpClientSession session(ReactiveSftpClientSession session, RootDirectory root) {
        RootedReactiveSftpOperations rooted = new RootedReactiveSftpOperations(session, root);
        return new ForwardingReactiveSftpClientSession() {
            @Override
            protected ReactiveSftpOperations delegate() {
                return rooted;
            }

            @Override
            protected ReactiveSftpClientSession session() {
                return session;
            }
        };
    }

    /** Maps {@code path} in, then runs the call — or fails the stage with the refusal. */
    private <T> CompletionStage<T> in(String path, Function<String, CompletionStage<T>> call) {
        String mapped;
        try {
            mapped = root.toServer(path);
        } catch (RuntimeException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        return call.apply(mapped);
    }

    private <T> CompletionStage<T> in(String first, String second,
            java.util.function.BiFunction<String, String, CompletionStage<T>> call) {
        String one;
        String two;
        try {
            one = root.toServer(first);
            two = root.toServer(second);
        } catch (RuntimeException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        return call.apply(one, two);
    }

    private CompletionStage<RemoteFile> translate(RemoteFile entry) {
        return root.fromServerAsync(entry.path(), delegate).thenApply(path -> new RemoteFile(
                "/".equals(path) ? "" : entry.name(), path, entry.size(), entry.directory(),
                entry.symlink(), entry.modified(), entry.permissions(), entry.ownerId(), entry.groupId()));
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return in(path, delegate::list).thenCompose(entries -> {
            List<CompletableFuture<RemoteFile>> translated = new ArrayList<>(entries.size());
            for (RemoteFile entry : entries) {
                translated.add(translate(entry).toCompletableFuture());
            }
            return CompletableFuture.allOf(translated.toArray(CompletableFuture[]::new))
                    .thenApply(done -> translated.stream().map(CompletableFuture::join).toList());
        });
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return in(path, delegate::stat).thenCompose(this::translate);
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return in(path, delegate::lstat).thenCompose(this::translate);
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return in(path, delegate::exists);
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return in(path, delegate::realpath)
                .thenCompose(answer -> root.fromServerAsync(SftpPaths.normalize(answer), delegate));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        String link;
        String target;
        try {
            link = root.toServer(linkPath);
            if (targetPath.startsWith("/")) {
                target = root.toServer(targetPath);
            } else {
                String normalisedLink = root.normalise(linkPath);
                int slash = normalisedLink.lastIndexOf('/');
                String parent = slash <= 0 ? "/" : normalisedLink.substring(0, slash);
                root.toServer(parent + "/" + targetPath); // checked; passed through as written
                target = targetPath;
            }
        } catch (RuntimeException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        return delegate.symlink(link, target);
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return in(linkPath, delegate::readlink).thenCompose(stored -> stored.startsWith("/")
                ? root.fromServerAsync(stored, delegate)
                : CompletableFuture.completedFuture(stored));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return in(linkPath, targetPath, delegate::hardlink);
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return in(path, mapped -> delegate.setAttributes(mapped, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return in(path, delegate::statvfs);
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return in(path, delegate::mkdir);
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return in(from, to, delegate::rename);
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return in(path, delegate::remove);
    }

    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        String mapped;
        try {
            mapped = root.toServer(remotePath);
        } catch (RuntimeException refused) {
            return subscriber -> {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                    }

                    @Override
                    public void cancel() {
                    }
                });
                subscriber.onError(refused);
            };
        }
        return delegate.download(mapped, fromOffset);
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        return in(remotePath, mapped -> delegate.upload(mapped, data, mode));
    }

    @Override
    public SftpCapabilities capabilities() {
        return delegate.capabilities();
    }
}
