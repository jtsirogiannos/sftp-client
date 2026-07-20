package io.github.jtsirogiannos.sftp.client.api;

import java.time.Instant;
import java.util.Objects;

/**
 * A directory entry or the result of a {@code stat}. Deliberately minimal: only attributes both
 * SFTP v3 and v6 can supply, so the same record is honest whichever provider produced it.
 *
 * <p>Notably absent is an owner/group *name*. SFTP v3 reports numeric uid/gid only, and resolving
 * them to names requires a shell round-trip that this library does not make. Exposing a name field
 * that is empty on JSch and populated on MINA is exactly the "silent difference in behaviour" the
 * capability model exists to prevent.
 *
 * @param name        the entry name with no directory component ({@code "a.txt"})
 * @param path        the absolute remote path ({@code "/upload/a.txt"})
 * @param size        size in bytes; 0 for directories
 * @param directory   true if this entry is a directory
 * @param symlink     true if this entry is a symbolic link; {@link #size()} then describes the link
 * @param modified    last-modified time, truncated to seconds on v3 servers
 * @param permissions POSIX permission bits ({@code 0644}), or {@code -1} if the server omitted them
 * @param ownerId     numeric uid, or {@link #UNKNOWN_ID} if the server omitted it. The read half of
 *                    {@link AttributeUpdate#ownerId()} — without it, ownership could be written but
 *                    never inspected or verified
 * @param groupId     numeric gid, or {@link #UNKNOWN_ID} if the server omitted it
 */
public record RemoteFile(
    String name,
    String path,
    long size,
    boolean directory,
    boolean symlink,
    Instant modified,
    int permissions,
    int ownerId,
    int groupId) {

    /**
     * Permission value used when the server did not report a mode.
     */
    public static final int UNKNOWN_PERMISSIONS = -1;

    /** Owner/group value used when the server did not report one. */
    public static final int UNKNOWN_ID = -1;

    public RemoteFile {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(path, "path");
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative: " + size);
        }
    }

    /**
     * Convenience for providers that have no mtime or mode to report.
     */
    public static RemoteFile of(String name, String path, long size, boolean directory) {
        return new RemoteFile(name, path, size, directory, false, Instant.EPOCH,
            UNKNOWN_PERMISSIONS, UNKNOWN_ID, UNKNOWN_ID);
    }

    /**
     * True when this is a plain file — neither a directory nor a symlink.
     */
    public boolean regularFile() {
        return !directory && !symlink;
    }
}
