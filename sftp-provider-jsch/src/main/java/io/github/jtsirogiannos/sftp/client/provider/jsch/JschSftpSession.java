package io.github.jtsirogiannos.sftp.client.provider.jsch;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;

/**
 * JSch-backed session: SFTP v3, stop-and-wait.
 *
 * <p>Like the MINA session, its real job is containment — {@code com.jcraft.jsch.SftpException} is
 * a <em>checked</em> exception with a public {@code id} field, and none of that shape is allowed to
 * reach a caller. Everything is translated to the library's unchecked {@link SftpException}, status
 * code preserved.
 */
final class JschSftpSession implements SftpSession {

    private final Session session;
    private final ChannelSftp channel;
    private volatile boolean closed;

    JschSftpSession(Session session, ChannelSftp channel) {
        this.session = session;
        this.channel = channel;
    }

    @Override
    public void list(String path, Predicate<RemoteFile> visitor) {
        call("list '" + path + "'", () -> {
            // JSch's own listing API is already a visitor: the selector returns CONTINUE or BREAK,
            // and BREAK stops it issuing further READDIRs. This maps across with no buffering.
            channel.ls(path, entry -> {
                String name = entry.getFilename();
                // JSch surfaces these; MINA does not. Filtering here is what keeps that difference
                // from becoming every caller's problem.
                if (".".equals(name) || "..".equals(name)) {
                    return ChannelSftp.LsEntrySelector.CONTINUE;
                }
                return visitor.test(toRemoteFile(name, join(path, name), entry.getAttrs()))
                        ? ChannelSftp.LsEntrySelector.CONTINUE
                        : ChannelSftp.LsEntrySelector.BREAK;
            });
            return null;
        });
    }

    @Override
    public RemoteFile stat(String path) {
        return call("stat '" + path + "'", () -> toRemoteFile(fileName(path), path, channel.stat(path)));
    }

    @Override
    public RemoteFile lstat(String path) {
        return call("lstat '" + path + "'", () -> toRemoteFile(fileName(path), path, channel.lstat(path)));
    }

    /**
     * Arguments are swapped on purpose. JSch's signature is {@code symlink(oldpath, newpath)} —
     * target first — while this SPI, like {@code Files.createSymbolicLink}, takes the link first.
     * Passing them through in order would create every link backwards.
     */
    @Override
    public void symlink(String linkPath, String targetPath) {
        call("symlink '" + linkPath + "' -> '" + targetPath + "'", () -> {
            channel.symlink(targetPath, linkPath);
            return null;
        });
    }

    @Override
    public String readlink(String linkPath) {
        return call("readlink '" + linkPath + "'", () -> channel.readlink(linkPath));
    }

    @Override
    public boolean exists(String path) {
        try {
            channel.stat(path);
            return true;
        } catch (com.jcraft.jsch.SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return false;
            }
            // Permission denied is a real failure; flattening it to false would tell callers a file
            // they simply cannot see does not exist.
            throw translate("exists '" + path + "'", e);
        }
    }

    @Override
    public void mkdir(String path) {
        call("mkdir '" + path + "'", () -> {
            channel.mkdir(path);
            return null;
        });
    }

    @Override
    public void rename(String from, String to) {
        call("rename '" + from + "' -> '" + to + "'", () -> {
            channel.rename(from, to);
            return null;
        });
    }

    @Override
    public void remove(String path) {
        call("remove '" + path + "'", () -> {
            // rm is files-only; rmdir is a separate request. The contract covers both, so the
            // provider decides. lstat rather than stat, so a symlink to a directory is unlinked
            // rather than treated as the directory it points at.
            SftpATTRS attrs = channel.lstat(path);
            if (attrs.isDir() && !attrs.isLink()) {
                channel.rmdir(path);
            } else {
                channel.rm(path);
            }
            return null;
        });
    }

    @Override
    public InputStream openRead(String path, long offset) {
        return call("openRead '" + path + "' at " + offset, () ->
                offset <= 0 ? channel.get(path) : channel.get(path, null, offset));
    }

    @Override
    public OutputStream openWrite(String path, WriteMode mode) {
        // Refused rather than emulated. JSch's put() exposes OVERWRITE/RESUME/APPEND and no way to
        // set SSH_FXF_EXCL, so the only "implementation" available is stat-then-create — a race that
        // silently corrupts data under concurrency. Reporting atomicCreateNew=false and throwing is
        // the honest option: the caller finds out at development time, not in production.
        if (mode == WriteMode.CREATE_NEW) {
            throw new UnsupportedOperationException(
                    "the jsch provider cannot honour CREATE_NEW atomically: JSch's client API has no "
                            + "SSH_FXF_EXCL equivalent. Check capabilities().atomicCreateNew() first, "
                            + "or use the mina provider.");
        }
        return call("openWrite '" + path + "'", () -> channel.put(path, jschMode(mode)));
    }

    /**
     * JSch speaks SFTP v3 and only v3, so nothing is negotiated and the provider's answer is already
     * the truth for every server.
     */
    @Override
    public SftpCapabilities capabilities() {
        return JschSftpProvider.CAPABILITIES;
    }

    @Override
    public String realpath(String path) {
        return call("realpath '" + path + "'", () -> channel.realpath(path));
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        if (update.isEmpty()) {
            return;
        }
        call("setAttributes '" + path + "'", () -> {
            // Read-modify-write, because JSch builds no SftpATTRS from scratch: the flags word on a
            // stat'd instance is what tells the server which fields to apply, and setters add to it.
            SftpATTRS attrs = channel.stat(path);
            update.permissions().ifPresent(attrs::setPERMISSIONS);
            update.size().ifPresent(attrs::setSIZE);
            update.modifiedTime().ifPresent(when ->
                    attrs.setACMODTIME(attrs.getATime(), (int) when.getEpochSecond()));
            if (update.ownerId().isPresent() || update.groupId().isPresent()) {
                attrs.setUIDGID(update.ownerId().orElse(attrs.getUId()),
                        update.groupId().orElse(attrs.getGId()));
            }
            channel.setStat(path, attrs);
            return null;
        });
    }

    /**
     * Declined, not attempted.
     *
     * <p>Hard links are {@code hardlink@openssh.com}, and JSch exposes no way to see which
     * extensions the server advertised — so this provider cannot know whether the call would work.
     * Reporting the capability as false and refusing is the honest reading of "what this session can
     * do"; guessing true would fail at runtime against any server without the extension. Use the
     * mina provider where hard links matter.
     */
    @Override
    public void hardlink(String linkPath, String targetPath) {
        throw new UnsupportedOperationException(
                "the jsch provider cannot determine whether the server supports "
                        + "hardlink@openssh.com, so it declines rather than guessing. "
                        + "Check capabilities().hardLinks(), or use the mina provider.");
    }

    /** Declined for the same reason as {@link #hardlink}: {@code statvfs@openssh.com} is an extension. */
    @Override
    public FilesystemStats statvfs(String path) {
        throw new UnsupportedOperationException(
                "the jsch provider cannot determine whether the server supports "
                        + "statvfs@openssh.com, so it declines rather than guessing. "
                        + "Check capabilities().statvfs(), or use the mina provider.");
    }

    @Override
    public boolean isValid() {
        return !closed && session.isConnected() && channel.isConnected();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            channel.exit();
        } catch (RuntimeException ignored) {
            // closing is not a failure mode worth propagating
        }
        try {
            session.disconnect();
        } catch (RuntimeException ignored) {
            // best effort
        }
    }

    private static int jschMode(WriteMode mode) {
        return switch (mode) {
            case OVERWRITE -> ChannelSftp.OVERWRITE;
            case APPEND -> ChannelSftp.APPEND;
            // Unreachable: rejected above, but the switch must stay exhaustive so that adding a
            // WriteMode later fails to compile here instead of silently overwriting.
            case CREATE_NEW -> throw new UnsupportedOperationException("CREATE_NEW is not supported");
        };
    }

    private static RemoteFile toRemoteFile(String name, String path, SftpATTRS attrs) {
        return new RemoteFile(
                name,
                path,
                attrs.isDir() ? 0 : Math.max(0, attrs.getSize()),
                attrs.isDir(),
                attrs.isLink(),
                // v3 mtimes are whole seconds, and JSch narrows them to a signed int: fine until
                // 2038, and there is nothing more precise on the wire to recover.
                Instant.ofEpochSecond(Integer.toUnsignedLong(attrs.getMTime())),
                attrs.getPermissions());
    }

    private static String join(String dir, String name) {
        return dir.endsWith("/") ? dir + name : dir + "/" + name;
    }

    private static String fileName(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    private <T> T call(String what, SftpOperation<T> op) {
        try {
            return op.run();
        } catch (com.jcraft.jsch.SftpException e) {
            throw translate(what, e);
        }
    }

    /** Carries JSch's {@code id} across as the SSH_FX status, so callers never parse messages. */
    private static SftpException translate(String what, com.jcraft.jsch.SftpException e) {
        return new SftpException(what + " failed: " + e.getMessage(), e, e.id);
    }

    @FunctionalInterface
    private interface SftpOperation<T> {
        T run() throws com.jcraft.jsch.SftpException;
    }
}
