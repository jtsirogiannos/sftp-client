package io.github.jtsirogiannos.sftp.client.api;

import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;

import java.util.UUID;
import java.util.concurrent.CompletionException;

/**
 * The parts of publish-by-rename that the blocking and reactive facades must agree on.
 *
 * <p>Both {@link SftpOperations#uploadAtomically} and {@link ReactiveSftpOperations#uploadAtomically}
 * are default methods composed from their own primitives, which is what keeps providers out of it.
 * The risk in having two implementations is that they drift — a different scratch-file suffix, or a
 * refusal message that explains the v3 limitation on one path and not the other. The rules live here
 * once so they cannot.
 */
final class AtomicPublish {

    private AtomicPublish() {
    }

    /**
     * A scratch path beside the destination.
     *
     * <p>Same directory, because rename is only atomic within a filesystem — a temp file under
     * {@code /tmp} would turn the publish into a copy and lose the whole guarantee. A leading dot so
     * a consumer globbing {@code *.csv} does not match the half-written copy, and a random component
     * so two writers publishing the same path do not share a scratch file and corrupt each other.
     */
    static String temporaryPathFor(String remotePath) {
        int slash = remotePath.lastIndexOf('/');
        String name = remotePath.substring(slash + 1);
        if (name.isEmpty()) {
            throw new SftpException("cannot upload to a directory path: " + remotePath);
        }
        String scratch = "." + name + "." + UUID.randomUUID() + ".part";
        if (slash < 0) {
            // A bare relative name lives in the login directory, so its scratch file must too. Treating
            // it as a child of "/" put the scratch file on another filesystem, or somewhere the account
            // cannot write, and turned the rename into a move.
            return scratch;
        }
        return remotePath.substring(0, slash + 1) + scratch;
    }

    /**
     * Why a v3 session will not replace an existing file this way.
     *
     * <p>Deliberately not a silent fallback to remove-then-rename: between those two calls the file
     * does not exist, and that gap is the exact window publish-by-rename exists to close. The JSch
     * provider already refuses to emulate {@code CREATE_NEW} with a stat-then-create race for the
     * same reason.
     */
    static UnsupportedOperationException cannotReplace(String remotePath,
            SftpCapabilities capabilities) {
        return new UnsupportedOperationException(
            "cannot atomically replace '" + remotePath + "': this session negotiated SFTP v"
                + capabilities.maxSftpVersion() + ", whose rename fails on an existing target. "
                + "Deleting the destination first would leave the window this method exists to "
                + "close. Check capabilities().posixRename() first, remove the file yourself if the "
                + "gap is acceptable, or use upload() if you do not need atomic publication.");
    }

    /**
     * Strips the {@link CompletionException} the JDK wraps around a failure that arrived through a
     * dependent stage, so a reactive caller catches the same type a blocking one would.
     */
    static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
    }
}
