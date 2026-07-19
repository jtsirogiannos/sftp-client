package io.github.jtsirogiannos.sftp.client.api;

import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Everything you can do to a remote filesystem, independent of how the connection is obtained.
 *
 * <p>Two things implement this. {@link SftpClient} borrows a pooled session per call and is safe to
 * share across threads. The view handed to {@link SftpClient#withSession} is bound to one session
 * for the duration of a block, so a sequence of operations travels over a single connection.
 *
 * <p>Splitting it out is what keeps the SPI free to move. {@link SftpSession} is the provider
 * contract, and it changes: this project has added {@code lstat}, {@code symlink}, {@code statvfs},
 * {@code hardlink}, {@code setAttributes} and {@code realpath} to it, and changed the signature of
 * {@code list}. Had callers been written against {@code SftpSession}, every one of those would have
 * been a breaking change for them rather than for the two providers.
 */
public interface SftpOperations {


    /**
     * Directory entries, excluding {@code .} and {@code ..}.
     */
    List<RemoteFile> list(String path);

    /**
     * Attributes of a single path. Throws if absent — use {@link #exists} to test.
     *
     * <p>Follows symlinks: a link reports as its target. Use {@link #lstat} to see the link itself.
     */
    RemoteFile stat(String path);

    /**
     * Attributes of a path without following symlinks.
     *
     * <p>The distinction matters whenever a link must not be treated as what it points at — see
     * {@link #deleteRecursively}.
     */
    RemoteFile lstat(String path);

    /**
     * Creates a symbolic link. Argument order is link-then-target, as in
     * {@link java.nio.file.Files#createSymbolicLink}.
     *
     * @throws UnsupportedOperationException if the provider reports no symlink support
     */
    void symlink(String linkPath, String targetPath);

    /**
     * The target of a symbolic link, exactly as stored — it may be relative.
     *
     * @throws UnsupportedOperationException if the provider reports no symlink support
     */
    String readlink(String linkPath);

    boolean exists(String path);

    /**
     * Resolves a path to its canonical absolute form, expanding {@code .}, {@code ..} and the
     * server's notion of {@code ~}.
     *
     * <p>{@code realpath(".")} answers the question every other method here assumes you already
     * know: which directory the connection actually starts in.
     */
    String realpath(String path);

    /** Changes permissions, size, timestamps or ownership. Only the fields set are sent. */
    void setAttributes(String path, AttributeUpdate update);

    /**
     * Creates a hard link. Argument order is link-then-target, as for {@link #symlink}.
     *
     * @throws UnsupportedOperationException if the session reports no hard-link support
     */
    void hardlink(String linkPath, String targetPath);

    /**
     * Space and inode figures for the filesystem holding {@code path}.
     *
     * @throws UnsupportedOperationException if the session reports no statvfs support
     */
    FilesystemStats statvfs(String path);

    /**
     * Copies a remote file into {@code target} starting {@code fromOffset} bytes in, and returns the
     * byte count.
     *
     * <p>What resuming an interrupted download looks like: pass the size already on disk. The
     * protocol has carried a read offset since v3, so this costs no extra round trips.
     */
    long download(String remotePath, OutputStream target, long fromOffset);

    /**
     * Creates a directory. Does not create intermediate parents.
     */
    void mkdir(String path);

    void rename(String from, String to);

    /**
     * Removes a file, or an empty directory. Not recursive.
     */
    void remove(String path);

    /**
     * Opens a remote file for reading.
     *
     * <p><strong>The caller must close the returned stream.</strong> The underlying session stays
     * checked out until then, and closing it is what releases the pool slot — an unclosed stream
     * leaks a connection. Prefer {@link #download(String, OutputStream)} unless you genuinely need
     * to drive the read yourself.
     */
    InputStream download(String remotePath);

    /**
     * Copies a remote file into {@code target} and returns the byte count. Closes its own session;
     * does <em>not</em> close {@code target}, which belongs to the caller.
     */
    long download(String remotePath, OutputStream target);

    /**
     * Uploads {@code data}, overwriting. Does not close {@code data}.
     */
    void upload(String remotePath, InputStream data);

    /**
     * Uploads {@code data} with explicit write semantics. Does not close {@code data}.
     */
    void upload(String remotePath, InputStream data, SftpSession.WriteMode mode);

    // --- convenience, built entirely from the primitives above ---
    //
    // These are default methods on purpose. Every one is expressible in terms of the operations a
    // provider already implements, so putting them in the SPI would add work for every driver author
    // and grow the surface the README asks to keep tiny — for no behaviour a provider could
    // implement better. Each borrows a pooled session per underlying call rather than holding one
    // for the whole traversal; with a real pool that is cheap, and the round trips are inherent to
    // the operation either way.

    /**
     * Uploads to a temporary name in the same directory and renames it into place, so a reader
     * watching the destination never sees a partially written file.
     *
     * <p>This is the fix for the oldest bug in SFTP integration: a drop folder, a producer writing
     * {@code orders.csv} straight to its final name, and a consumer that picks the file up when it
     * is a third written. Ordinary {@link #upload} cannot avoid it — the file is visible under its
     * real name from the first byte. Publishing by rename means the destination goes from absent (or
     * the previous version) to complete, with nothing in between.
     *
     * <p>The temporary name is a dotfile ending in {@code .part}, in the <em>same directory</em> as
     * the destination — rename is only atomic within a filesystem, so a temp file in {@code /tmp}
     * would defeat the purpose. It is removed on a best-effort basis if the upload fails.
     *
     * <h4>When the server cannot do this</h4>
     *
     * <p>Replacing an <em>existing</em> file this way needs a rename that overwrites, which is
     * {@code posix-rename} and therefore SFTP v5 or later — see
     * {@link io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities#posixRename()}. On a v3 server
     * a rename onto an occupied path simply fails, and the only way through would be to delete the
     * destination first and rename into the gap. That gap is exactly the window this method exists to
     * close, so it is not done: the call throws {@link UnsupportedOperationException} instead, the
     * same way the JSch provider refuses to emulate {@code CREATE_NEW} with a stat-then-create race.
     * Writing to a path that does not exist yet is atomic on every version and always allowed.
     *
     * <p>The existence check is for the error message, not for correctness — the destination could
     * appear between the check and the rename. On a v3 server that rename then fails loudly, which is
     * the right outcome; nothing silently overwrites.
     *
     * @throws UnsupportedOperationException if the destination exists and the session cannot rename
     *                                       atomically over it
     */
    default void uploadAtomically(String remotePath, InputStream data) {
        String temporary = AtomicPublish.temporaryPathFor(remotePath);
        if (exists(remotePath) && !capabilities().posixRename()) {
            throw AtomicPublish.cannotReplace(remotePath, capabilities());
        }
        try {
            upload(temporary, data);
            rename(temporary, remotePath);
        } catch (RuntimeException e) {
            try {
                remove(temporary);
            } catch (RuntimeException cleanupFailed) {
                // The original failure is the one worth reporting; a leftover .part file is visible
                // and harmless, whereas swallowing the real cause is neither.
                e.addSuppressed(cleanupFailed);
            }
            throw e;
        }
    }

    /**
     * Creates a directory and any missing parents, like {@code mkdir -p}.
     *
     * <p>{@link #mkdir} fails when the parent is absent, which makes "write this file into a dated
     * folder" unexpectedly fiddly — the reason nearly every caller ends up writing this loop.
     *
     * <p>Already-existing directories are not an error. A concurrent creator that wins the race is
     * treated the same way: if the path exists afterwards, the goal is met.
     */
    default void mkdirs(String path) {
        String normalised = path.endsWith("/") && path.length() > 1
                ? path.substring(0, path.length() - 1)
                : path;
        if (normalised.isEmpty() || "/".equals(normalised) || exists(normalised)) {
            return;
        }

        StringBuilder progress = new StringBuilder();
        for (String segment : normalised.split("/")) {
            if (segment.isEmpty()) {
                continue;
            }
            progress.append('/').append(segment);
            String current = progress.toString();
            if (exists(current)) {
                continue;
            }
            try {
                mkdir(current);
            } catch (SftpException e) {
                // Someone else may have created it between the check and the call.
                if (!exists(current)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Removes a file, or a directory and everything beneath it.
     *
     * <p>{@link #remove} deletes only files and empty directories, so clearing a tree otherwise
     * means hand-rolling the traversal — and getting the order wrong, or following a symlink out of
     * the tree, are both easy mistakes.
     *
     * <p><strong>Symlinks are removed, never followed.</strong> Descending into one would delete
     * files outside the tree the caller named, which is the kind of surprise that ends up in an
     * incident report.
     *
     * <p>Not atomic. A failure partway leaves the tree partly deleted, and the exception says which
     * path failed. Nothing in SFTP offers better.
     *
     * @return the number of entries removed, directories included
     */
    default int deleteRecursively(String path) {
        if ("/".equals(path)) {
            // Refusing outright: nothing good follows from a typo'd variable expanding to "/".
            throw new SftpException("refusing to delete the remote root directory");
        }
        if (!exists(path)) {
            return 0;
        }

        // lstat, emphatically not stat: stat follows the link and reports the *target*, so
        // entry.symlink() would always be false and this would recurse into whatever the link
        // points at. That is the difference between clearing a directory and deleting a filesystem.
        RemoteFile entry = lstat(path);
        // A symlink is deleted as the link it is; its target is somebody else's tree.
        if (!entry.directory() || entry.symlink()) {
            remove(path);
            return 1;
        }

        int removed = 0;
        for (RemoteFile child : list(path)) {
            removed += deleteRecursively(child.path());
        }
        remove(path); // now empty
        return removed + 1;
    }

    /** Sets POSIX mode bits — the common case of {@link #setAttributes}. Write it in octal. */
    default void chmod(String path, int mode) {
        setAttributes(path, AttributeUpdate.builder().permissions(mode).build());
    }

    /**
     * Downloads to a local file, creating or truncating it.
     *
     * @return the number of bytes written
     */
    default long download(String remotePath, java.nio.file.Path localFile) {
        try (OutputStream out = java.nio.file.Files.newOutputStream(localFile)) {
            return download(remotePath, out);
        } catch (java.io.IOException e) {
            throw new SftpException("could not write '" + localFile + "'", e);
        }
    }

    /** Uploads a local file, overwriting the remote path. */
    default void upload(java.nio.file.Path localFile, String remotePath) {
        try (InputStream in = java.nio.file.Files.newInputStream(localFile)) {
            upload(remotePath, in);
        } catch (java.io.IOException e) {
            throw new SftpException("could not read '" + localFile + "'", e);
        }
    }

    /**
     * Walks a directory, handing each entry to {@code visitor} as it arrives; return {@code false} to
     * stop.
     *
     * <p>The streaming counterpart to {@link #list(String)}, and the one to reach for when a
     * directory might be large or the answer might come early. SFTP lists a directory in pages, so
     * this is closer to what the protocol does than a materialised list is.
     *
     * <p>A visitor is used rather than a returned {@code Stream} on purpose. A lazy stream would
     * hold an open directory handle and a checked-out pooled session until somebody closed it, and a
     * {@code Stream} looks like a value rather than a resource — the leak would be easy to write and
     * hard to see. Here the library owns the handle for the duration of the call.
     *
     * <p><strong>Stopping early costs the session.</strong> Entries the server has already queued are
     * discarded, so the session is not returned to the pool — the same trade an abandoned download
     * makes. Walking to the end is cheaper if you need everything anyway.
     *
     * <p>This default implementation is correct but eager: it materialises the listing first.
     * {@code DefaultSftpClient} overrides it to stream, which is where the benefit actually comes
     * from.
     */
    default void visit(String path, java.util.function.Predicate<RemoteFile> visitor) {
        for (RemoteFile entry : list(path)) {
            if (!visitor.test(entry)) {
                return;
            }
        }
    }

    /**
     * Directory entries matching a predicate.
     *
     * <p>Filtered client-side — SFTP has no server-side glob, so the whole listing still crosses the
     * wire. It does not, however, hold the whole listing in memory: entries are tested as they arrive
     * and only the matches are kept.
     */
    default List<RemoteFile> list(String path, java.util.function.Predicate<RemoteFile> filter) {
        List<RemoteFile> matches = new java.util.ArrayList<>();
        visit(path, entry -> {
            if (filter.test(entry)) {
                matches.add(entry);
            }
            return true;
        });
        return matches;
    }

    /**
     * The first entry matching {@code predicate}, stopping the walk as soon as it is found.
     *
     * <p>The case a materialised listing serves worst: finding one file in a large drop directory
     * should not mean transferring, allocating and discarding the other hundred thousand.
     */
    default java.util.Optional<RemoteFile> findFirst(String path,
                                                     java.util.function.Predicate<RemoteFile> predicate) {
        RemoteFile[] found = new RemoteFile[1];
        visit(path, entry -> {
            if (predicate.test(entry)) {
                found[0] = entry;
                return false;
            }
            return true;
        });
        return java.util.Optional.ofNullable(found[0]);
    }

    /**
     * What the underlying provider can do. Constant for the life of the client.
     */
    SftpCapabilities capabilities();
}
