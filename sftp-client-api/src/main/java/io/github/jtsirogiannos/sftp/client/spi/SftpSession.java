package io.github.jtsirogiannos.sftp.client.spi;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Predicate;

/**
 * One live SFTP channel. Blocking and <strong>not thread-safe</strong>: a session is owned by exactly
 * one caller between {@code borrow} and {@code recycle}, which is what lets providers wrap a
 * single-threaded backend channel without adding locks.
 *
 * <p>Provider authors implement this and {@link SftpProvider}, and nothing else. Everything
 * reactive — publishers, demand accounting, executors — lives in the core and is built on top of
 * these blocking primitives. Keeping the SPI blocking is what makes writing a third-party provider a
 * weekend job rather than a research project.
 *
 * <p><strong>All failures must surface as {@link SftpException}.</strong> A backend exception
 * reaching a caller makes the provider choice visible in user code, which is the one thing this
 * design exists to prevent. The TCK enforces this.
 */
public interface SftpSession extends SftpSessionLifecycle {

    /**
     * Walks a directory, handing each entry to {@code visitor} as it arrives.
     *
     * <p>This is the primitive, and {@link #list(String)} is derived from it, because the protocol
     * works this way: {@code SSH_FXP_OPENDIR}, then {@code READDIR} until the server stops, then
     * {@code CLOSE}. A materialised list is the abstraction that hides the paging; a visitor is the
     * one both backends implement without emulation — JSch's {@code LsEntrySelector} already returns
     * continue-or-break, and MINA pages through a handle.
     *
     * <p>Returning {@code false} stops the walk. The provider must then close the directory handle
     * before returning: entries the server has already queued are dropped, and the session is left
     * usable. Callers who stop early should expect the session to be discarded rather than pooled,
     * for the same reason an abandoned download is.
     *
     * <p>{@code .} and {@code ..} are never visited — the TCK asserts their absence, because JSch
     * reports them and MINA does not, and leaking that difference would mean every user writing the
     * same filter.
     */
    void list(String path, Predicate<RemoteFile> visitor);

    /**
     * Every entry in a directory, collected.
     *
     * <p>Convenient and usually right, but it holds the whole listing in memory — a drop directory
     * with a few hundred thousand files is a real thing in batch workflows. Use
     * {@link #list(String, Predicate)} when the directory might be large or you can stop early.
     */
    default List<RemoteFile> list(String path) {
        List<RemoteFile> entries = new java.util.ArrayList<>();
        list(path, entry -> {
            entries.add(entry);
            return true;
        });
        return entries;
    }

    /**
     * Attributes of a single path, <strong>following symlinks</strong>: a link is reported as
     * whatever it points at, and {@link RemoteFile#symlink()} is therefore always false here. Use
     * {@link #lstat} to see the link itself.
     *
     * @throws io.github.jtsirogiannos.sftp.client.api.SftpException with status
     *         {@code SSH_FX_NO_SUCH_FILE} if the path does not exist — use {@link #exists} to test
     *         rather than catching this
     */
    RemoteFile stat(String path);

    /**
     * Attributes of a path <strong>without following symlinks</strong>.
     *
     * <p>{@link #stat} follows them, so it reports a link as whatever it points at and its
     * {@link RemoteFile#symlink()} is always false. Anything that must distinguish a link from its
     * target has to use this instead — a recursive delete that gets it wrong follows a link out of
     * the tree it was told to clear and deletes somebody else's files.
     */
    RemoteFile lstat(String path);

    /**
     * True if the path exists. Implemented as a {@code stat} whose "no such file" status is caught,
     * so it is one round-trip; any other failure still propagates.
     */
    boolean exists(String path);

    /**
     * Creates a symbolic link.
     *
     * <p><strong>Argument order is link-then-target</strong>, matching
     * {@link java.nio.file.Files#createSymbolicLink}. Worth stating explicitly because the backends
     * disagree: MINA's {@code symLink} takes {@code (linkPath, targetPath)} while JSch's
     * {@code symlink} takes {@code (oldpath, newpath)} — that is, target first. A provider that
     * passes them straight through creates every link backwards.
     *
     * @param linkPath   where the link is created
     * @param targetPath what it points at
     * @throws UnsupportedOperationException if {@link #capabilities()} reports no symlink support
     */
    void symlink(String linkPath, String targetPath);

    /**
     * The target of a symbolic link, exactly as stored — it may be relative.
     *
     * @throws SftpException if the path is not a symlink
     * @throws UnsupportedOperationException if {@link #capabilities()} reports no symlink support
     */
    String readlink(String linkPath);

    /**
     * Resolves a path to its canonical absolute form ({@code SSH_FXP_REALPATH}).
     *
     * <p>Expands {@code .}, {@code ..} and any server-side notion of {@code ~}, and is the only way
     * to ask where a relative path actually lands — including the directory the session starts in,
     * which {@code realpath(".")} answers. Every other method here assumes you already know the
     * absolute path; this is how you find it out.
     */
    String realpath(String path);

    /**
     * Changes permissions, size, timestamps or ownership ({@code SSH_FXP_SETSTAT}).
     *
     * <p>Only the fields set on the update are sent, matching the protocol's flags word — see
     * {@link AttributeUpdate}. An empty update is a no-op.
     */
    void setAttributes(String path, AttributeUpdate update);

    /**
     * Creates a hard link.
     *
     * <p>{@code hardlink@openssh.com}, an extension rather than part of any SFTP draft, so it is
     * gated on {@link SftpCapabilities#hardLinks()}. Argument order matches {@link #symlink} —
     * link first — and JSch's underlying call takes them the other way round.
     *
     * @throws UnsupportedOperationException if the session reports no hard-link support
     */
    void hardlink(String linkPath, String targetPath);

    /**
     * Space and inode figures for the filesystem holding {@code path}.
     *
     * <p>{@code statvfs@openssh.com}, likewise an extension, gated on
     * {@link SftpCapabilities#statvfs()}.
     *
     * @throws UnsupportedOperationException if the session reports no statvfs support
     */
    FilesystemStats statvfs(String path);

    /**
     * Creates a single directory. Does not create intermediate parents; the core's {@code mkdirs}
     * is built from this.
     *
     * @throws io.github.jtsirogiannos.sftp.client.api.SftpException with
     *         {@code SSH_FX_NO_SUCH_FILE} if the parent is missing, or
     *         {@code SSH_FX_FILE_ALREADY_EXISTS} if the path is taken
     */
    void mkdir(String path);

    /**
     * Renames or moves. Whether this atomically replaces an existing target depends on the server:
     * see {@link SftpCapabilities#posixRename()}. When that flag is false, a rename onto an existing
     * path fails, and the core will not emulate the difference behind your back.
     */
    void rename(String from, String to);

    /**
     * Removes a file <em>or</em> an empty directory. Not recursive.
     *
     * <p><strong>Implementer note, and the one most easily got wrong:</strong> the protocol uses two
     * different packets — {@code SSH_FXP_REMOVE} for files and {@code SSH_FXP_RMDIR} for directories
     * — and most client libraries expose them as two methods. A provider that only ever sends the
     * file form works perfectly until someone deletes a directory. Decide which to send by checking
     * the path first, with {@link #lstat} rather than {@link #stat}, so a symlink to a directory is
     * removed as the link it is instead of being followed.
     *
     * @throws io.github.jtsirogiannos.sftp.client.api.SftpException if the path is missing, or is a
     *         directory that still has entries
     */
    void remove(String path);

    /**
     * Opens a remote file for reading. The caller closes the stream; closing it must not close the
     * session. The core wraps this in a {@code Flow.Publisher}, so the stream is read from a worker
     * thread and may be abandoned mid-transfer on cancellation.
     */
    default InputStream openRead(String path) {
        return openRead(path, 0);
    }

    /**
     * Opens a remote file for reading from {@code offset} bytes in.
     *
     * <p>{@code SSH_FXP_READ} has always carried an offset, so resuming an interrupted download
     * costs nothing extra on the wire — it is only the API that has to expose it. An offset past the
     * end of the file yields an empty stream rather than an error.
     */
    InputStream openRead(String path, long offset);

    /**
     * Opens a remote file for writing. The caller closes the stream, and close must flush — an
     * upload is not durable until then.
     *
     * <p>{@link WriteMode#CREATE_NEW} must either be honoured atomically by the backend or refused:
     * report {@link SftpCapabilities#atomicCreateNew()} false and throw
     * {@link UnsupportedOperationException}. Emulating it with a stat-then-create sequence loses the
     * exclusivity the caller asked for, and is worse than not offering it.
     *
     * @throws UnsupportedOperationException if {@code mode} is one this backend cannot honour
     */
    OutputStream openWrite(String path, WriteMode mode);


    /**
     * How {@link SftpSession#openWrite} treats an existing path.
     */
    enum WriteMode {

        /**
         * Truncate if present, create if not.
         */
        OVERWRITE,

        /**
         * Create, failing with {@link SftpException} if the path already exists. The check and the
         * create must be atomic on the server ({@code SSH_FXF_EXCL}) — a stat-then-create in the
         * provider is a race, and the TCK exists partly to catch that shortcut.
         */
        CREATE_NEW,

        /**
         * Append to an existing file, creating it if absent.
         */
        APPEND
    }
}
