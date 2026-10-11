package io.github.jtsirogiannos.sftp.client.api;

import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Everything you can do to a remote filesystem without blocking, independent of how the connection
 * is obtained — the non-blocking twin of {@link SftpOperations}, in JDK types only:
 * {@link CompletionStage} and {@link Flow.Publisher}.
 *
 * <p>Two things implement it. {@link ReactiveSftpClient} borrows a pooled session per call and is
 * safe to share; a session is pinned to one connection until closed.
 * Splitting it out is what lets a unit of work hand its block exactly these operations and nothing
 * that belongs to owning a pool.
 *
 * <p>Honesty note carried over from the capability model: {@link #download}/{@link #upload} are
 * demand-driven and backpressured, but the pull always happens on a worker thread. When
 * {@link #capabilities()} reports {@code pipelinedStreaming} the provider overlaps requests so that
 * worker rarely stalls — it is not zero-thread async, and this API never pretends it is.
 */
public interface ReactiveSftpOperations {


    CompletionStage<List<RemoteFile>> list(String path);

    CompletionStage<RemoteFile> stat(String path);

    CompletionStage<Boolean> exists(String path);

    CompletionStage<Void> mkdir(String path);

    CompletionStage<Void> rename(String from, String to);

    CompletionStage<Void> remove(String path);

    /**
     * Attributes of a path <strong>without following symlinks</strong> — the counterpart to
     * {@link #stat}, and what anything walking a tree has to use.
     */
    CompletionStage<RemoteFile> lstat(String path);

    /** Resolves a path to its canonical absolute form, expanding {@code .}, {@code ..} and home. */
    CompletionStage<String> realpath(String path);

    /** Creates a symbolic link at {@code linkPath} pointing at {@code targetPath}. */
    CompletionStage<Void> symlink(String linkPath, String targetPath);

    /** The target a symbolic link points at, unresolved. */
    CompletionStage<String> readlink(String linkPath);

    /** Creates a hard link. Requires {@link SftpCapabilities#hardLinks()}. */
    CompletionStage<Void> hardlink(String linkPath, String targetPath);

    /** Applies a partial attribute update, leaving unset fields alone. */
    CompletionStage<Void> setAttributes(String path, AttributeUpdate update);

    /** Filesystem space and inode counts. Requires {@link SftpCapabilities#statvfs()}. */
    CompletionStage<FilesystemStats> statvfs(String path);

    /**
     * Streams a remote file as backpressured chunks. Cancelling the subscription interrupts the
     * transfer and discards the underlying session rather than returning it to the pool.
     *
     * <p>Buffers are handed off to the subscriber and not reused, so they are safe to retain.
     */
    default Flow.Publisher<ByteBuffer> download(String remotePath) {
        return download(remotePath, 0);
    }

    /**
     * As {@link #download(String)}, starting at {@code fromOffset} — the resume primitive. Bytes
     * before the offset are never transferred.
     */
    Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset);

    /**
     * Consumes {@code data} with backpressure and writes it to the remote path, completing when the
     * upstream completes and the final flush lands.
     *
     * <p><strong>Cancelling the returned stage cancels the transfer.</strong> The upstream
     * subscription is cancelled, the remote file is closed and the session is discarded rather than
     * pooled. This is what makes a cancelled coroutine or a disposed {@code Mono} actually stop the
     * work rather than leave it running against a result nobody is waiting for.
     *
     * <p>One chunk may still land after cancelling: demand is one buffer at a time, and a write
     * already handed to the stream cannot be recalled. Nothing further is requested or written.
     */
    default CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data) {
        return upload(remotePath, data, SftpSession.WriteMode.OVERWRITE);
    }

    /**
     * As {@link #upload(String, Flow.Publisher)}, with explicit write semantics — {@code APPEND},
     * or {@code CREATE_NEW} where {@link SftpCapabilities#atomicCreateNew()} allows it. The
     * two-argument form overwrites.
     */
    CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode);

    /**
     * Publishes by rename: uploads to a scratch name beside the destination and renames it into
     * place, so a reader watching the destination never sees a partially written file.
     *
     * <p>The reactive counterpart of {@link SftpOperations#uploadAtomically}, with the same promise
     * and the same refusal. A default method composed from {@link #exists}, {@link #upload},
     * {@link #rename} and {@link #remove}, so both the worker-offloaded and the thread-free client
     * inherit it and neither implements anything.
     *
     * <p>Replacing an existing file needs a rename that overwrites — {@code posix-rename}, SFTP v5 or
     * later. Against a v3 server the returned stage fails with {@link UnsupportedOperationException}
     * rather than falling back to remove-then-rename, because that gap is the window this method
     * exists to close. Publishing to a path that does not exist yet is atomic on every version.
     *
     * <p>{@code data} is subscribed at most once, so a failure is never retried behind the caller's
     * back. If the upload or the rename fails, the scratch file is removed on a best-effort basis and
     * the original failure is what the stage reports; a cleanup failure is attached to it as a
     * suppressed exception rather than replacing it.
     */
    default CompletionStage<Void> uploadAtomically(String remotePath,
            Flow.Publisher<ByteBuffer> data) {
        // Even the argument check fails the stage rather than throwing: a caller composing stages
        // has no way to catch a synchronous throw, which is this interface's own stated rule —
        // and the reason deleteRecursively's root refusal is a failedFuture too.
        String temporary;
        try {
            temporary = AtomicPublish.temporaryPathFor(remotePath);
        } catch (RuntimeException invalidPath) {
            return CompletableFuture.failedFuture(invalidPath);
        }
        return exists(remotePath).thenCompose(present -> {
            if (present && !capabilities().posixRename()) {
                return CompletableFuture.<Void>failedFuture(
                    AtomicPublish.cannotReplace(remotePath, capabilities()));
            }
            return upload(temporary, data)
                .thenCompose(uploaded -> rename(temporary, remotePath))
                // handle() rather than exceptionally(): the cleanup has to run on the failure path
                // without swallowing the success path, and it is itself asynchronous.
                .handle((published, failure) -> failure)
                .thenCompose(failure -> failure == null
                    ? CompletableFuture.<Void>completedFuture(null)
                    : cleanUpThenFail(temporary, AtomicPublish.unwrap(failure)));
        });
    }

    /** Best-effort removal of the scratch file, then re-fails with the original cause. */
    private CompletionStage<Void> cleanUpThenFail(String temporary, Throwable original) {
        return remove(temporary)
            .handle((removed, cleanupFailure) -> cleanupFailure)
            .thenCompose(cleanupFailure -> {
                if (cleanupFailure != null) {
                    // A leftover .part file is visible and harmless; losing the real cause is not.
                    original.addSuppressed(AtomicPublish.unwrap(cleanupFailure));
                }
                return CompletableFuture.<Void>failedFuture(original);
            });
    }

    // --- convenience, composed entirely from the operations above ---
    //
    // Default methods for the same reason as their blocking counterparts: anything expressible from
    // the primitives stays out of the implementations, so neither reactive client writes them and a
    // future one gets them free.

    /** Directory entries matching a predicate. Filtered client-side; SFTP has no server-side glob. */
    default CompletionStage<List<RemoteFile>> list(String path, Predicate<RemoteFile> filter) {
        return list(path).thenApply(entries -> entries.stream().filter(filter).toList());
    }

    /**
     * The first entry matching {@code match}, if any.
     *
     * <p>Unlike {@link SftpOperations#findFirst}, this fetches the whole listing before filtering:
     * the blocking form can stop the walk early because it drives the visitor itself, and there is
     * no equivalent here while {@link #list} returns a completed list rather than a stream. For a
     * directory large enough that the difference matters, prefer streaming the listing yourself.
     */
    default CompletionStage<Optional<RemoteFile>> findFirst(String path,
            Predicate<RemoteFile> match) {
        return list(path).thenApply(entries -> entries.stream().filter(match).findFirst());
    }

    /** Sets permissions, the common case of {@link #setAttributes}. */
    default CompletionStage<Void> chmod(String path, int mode) {
        return setAttributes(path, AttributeUpdate.builder().permissions(mode).build());
    }

    /**
     * Creates a directory and any missing parents, like {@code mkdir -p}.
     *
     * <p>An already-existing directory is not an error, and neither is losing the race to a
     * concurrent creator: if the path exists afterwards, the goal is met.
     */
    default CompletionStage<Void> mkdirs(String path) {
        String normalised = path.endsWith("/") && path.length() > 1
            ? path.substring(0, path.length() - 1)
            : path;
        if (normalised.isEmpty() || "/".equals(normalised)) {
            return CompletableFuture.completedFuture(null);
        }
        // Built iteratively rather than by recursing through thenCompose: against a provider whose
        // stages are already complete the composition runs inline, and a recursive chain would grow
        // the stack once per path segment.
        CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
        // A relative path stays relative, as in the blocking form.
        StringBuilder progress = new StringBuilder(normalised.startsWith("/") ? "/" : "");
        for (String segment : normalised.split("/")) {
            if (segment.isEmpty()) {
                continue;
            }
            if (progress.length() > 0 && progress.charAt(progress.length() - 1) != '/') {
                progress.append('/');
            }
            progress.append(segment);
            String current = progress.toString();
            chain = chain.thenCompose(ignored -> createIfAbsent(current));
        }
        return chain;
    }

    private CompletionStage<Void> createIfAbsent(String path) {
        return exists(path).thenCompose(present -> present
            ? CompletableFuture.<Void>completedFuture(null)
            : mkdir(path)
                .handle((created, failure) -> failure)
                .thenCompose(failure -> failure == null
                    ? CompletableFuture.<Void>completedFuture(null)
                    // Someone else may have created it between the check and the call.
                    : exists(path).thenCompose(nowPresent -> nowPresent
                        ? CompletableFuture.<Void>completedFuture(null)
                        : CompletableFuture.<Void>failedFuture(AtomicPublish.unwrap(failure)))));
    }

    /**
     * Deletes a path and everything beneath it, returning how many entries were removed.
     *
     * <p><strong>Symlinks are removed as links, never followed.</strong> That is why this walks with
     * {@link #lstat} and not {@link #stat}: {@code stat} resolves a link and reports the target, so
     * a link to a directory would look like a directory and the walk would descend out of the tree
     * it was given and delete somebody else's files.
     */
    default CompletionStage<Integer> deleteRecursively(String path) {
        // The same refusals as the blocking form, failing the stage rather than throwing.
        SftpException refusal = SftpPaths.refusalToDeleteRecursively(path);
        if (refusal != null) {
            return CompletableFuture.failedFuture(refusal);
        }
        return exists(path).thenCompose(present -> {
            if (!present) {
                return CompletableFuture.completedFuture(0);
            }
            return lstat(path).thenCompose(entry -> {
                if (!entry.directory() || entry.symlink()) {
                    return remove(path).thenApply(removed -> 1);
                }
                return list(path)
                    .thenCompose(this::sequentially)
                    .thenCompose(beneath -> remove(path).thenApply(removed -> beneath + 1));
            });
        });
    }

    /** Folds the children iteratively, so breadth does not become stack depth. */
    private CompletionStage<Integer> sequentially(List<RemoteFile> children) {
        CompletionStage<Integer> chain = CompletableFuture.completedFuture(0);
        for (RemoteFile child : children) {
            String childPath = child.path();
            chain = chain.thenCompose(
                count -> deleteRecursively(childPath).thenApply(removed -> count + removed));
        }
        return chain;
    }

    SftpCapabilities capabilities();
}
