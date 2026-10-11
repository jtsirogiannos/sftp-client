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
 *
 * <h2>Paths are patterns to JSch, and must not be</h2>
 *
 * <p>Nearly every {@link ChannelSftp} method runs its path through a glob: {@code *} and {@code ?}
 * match, and a backslash escapes. So {@code rm("/d/*")} deletes every file in {@code /d}, a file
 * literally named {@code a*b} cannot be opened, and {@code /x/.\./.\./etc} is unescaped into
 * {@code /x/../../etc} after any lexical check a caller made has passed. The SPI promises literal
 * paths, so every path handed to a globbing method goes through {@link #literal} first.
 * {@code mkdir} and {@code realpath} do not glob, and quoting them would put the backslashes on the
 * wire — they take the path unchanged.
 */
final class JschSftpSession implements SftpSession {

    private final Session session;
    private final ChannelSftp channel;
    private volatile boolean closed;

    /**
     * Set when a failure may have left the channel out of step with the server.
     *
     * <p>JSch reports two very different things as status {@code SSH_FX_FAILURE}: a server that
     * answered "failure", and an exception thrown partway through reading a reply — an I/O error, a
     * protocol surprise. The first leaves the channel perfectly usable. The second can leave unread
     * bytes in the channel, which the next request would read as its own answer, while the socket
     * still reports itself connected. The two are told apart by the cause: a status error from the
     * server never carries one.
     */
    private volatile boolean broken;

    JschSftpSession(Session session, ChannelSftp channel) {
        this.session = session;
        this.channel = channel;
    }

    @Override
    public void list(String path, Predicate<RemoteFile> visitor) {
        Throwable[] visitorFailure = new Throwable[1];
        call("list '" + path + "'", () -> {
            // JSch's own listing API is already a visitor: the selector returns CONTINUE or BREAK,
            // and BREAK stops it issuing further READDIRs. This maps across with no buffering.
            channel.ls(literal(path), entry -> {
                String name = entry.getFilename();
                // JSch surfaces these; MINA does not. Filtering here is what keeps that difference
                // from becoming every caller's problem.
                if (".".equals(name) || "..".equals(name)) {
                    return ChannelSftp.LsEntrySelector.CONTINUE;
                }
                try {
                    return visitor.test(toRemoteFile(name, join(path, name), entry.getAttrs()))
                            ? ChannelSftp.LsEntrySelector.CONTINUE
                            : ChannelSftp.LsEntrySelector.BREAK;
                } catch (Throwable failure) {
                    // Not allowed to escape from here: JSch would abandon the listing halfway
                    // through a NAME packet, leaving its remaining bytes in the channel and the
                    // directory handle open. BREAK drains the packet and closes the handle, so the
                    // visitor's failure costs the caller nothing but the walk.
                    visitorFailure[0] = failure;
                    return ChannelSftp.LsEntrySelector.BREAK;
                }
            });
            return null;
        });
        if (visitorFailure[0] instanceof RuntimeException e) {
            throw e;
        }
        if (visitorFailure[0] instanceof Error e) {
            throw e;
        }
        if (visitorFailure[0] != null) {
            // A checked exception smuggled through a Predicate. Not ours to translate into an SFTP
            // failure, but it has to surface as something unchecked.
            throw new java.lang.reflect.UndeclaredThrowableException(visitorFailure[0]);
        }
    }

    @Override
    public RemoteFile stat(String path) {
        return call("stat '" + path + "'", () -> toRemoteFile(fileName(path), path,
                channel.stat(literal(path))));
    }

    @Override
    public RemoteFile lstat(String path) {
        return call("lstat '" + path + "'", () -> toRemoteFile(fileName(path), path,
                channel.lstat(literal(path))));
    }

    /**
     * Arguments are swapped on purpose. JSch's signature is {@code symlink(oldpath, newpath)} —
     * target first — while this SPI, like {@code Files.createSymbolicLink}, takes the link first.
     * Passing them through in order would create every link backwards.
     */
    @Override
    public void symlink(String linkPath, String targetPath) {
        call("symlink '" + linkPath + "' -> '" + targetPath + "'", () -> {
            channel.symlink(literal(targetPath), literal(linkPath));
            return null;
        });
    }

    @Override
    public String readlink(String linkPath) {
        return call("readlink '" + linkPath + "'", () -> channel.readlink(literal(linkPath)));
    }

    @Override
    public boolean exists(String path) {
        try {
            channel.stat(literal(path));
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
            channel.rename(literal(from), literal(to));
            return null;
        });
    }

    @Override
    public void remove(String path) {
        call("remove '" + path + "'", () -> {
            // rm is files-only; rmdir is a separate request. The contract covers both, so the
            // provider decides. lstat rather than stat, so a symlink to a directory is unlinked
            // rather than treated as the directory it points at.
            SftpATTRS attrs = channel.lstat(literal(path));
            if (attrs.isDir() && !attrs.isLink()) {
                channel.rmdir(literal(path));
            } else {
                channel.rm(literal(path));
            }
            return null;
        });
    }

    @Override
    public InputStream openRead(String path, long offset) {
        return call("openRead '" + path + "' at " + offset, () ->
                offset <= 0 ? channel.get(literal(path)) : channel.get(literal(path), null, offset));
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
        return call("openWrite '" + path + "'", () -> channel.put(literal(path), jschMode(mode)));
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
            SftpATTRS attrs = channel.stat(literal(path));
            update.permissions().ifPresent(attrs::setPERMISSIONS);
            update.size().ifPresent(attrs::setSIZE);
            update.modifiedTime().ifPresent(when ->
                    attrs.setACMODTIME(attrs.getATime(), (int) when.getEpochSecond()));
            if (update.ownerId().isPresent() || update.groupId().isPresent()) {
                attrs.setUIDGID(update.ownerId().orElse(attrs.getUId()),
                        update.groupId().orElse(attrs.getGId()));
            }
            channel.setStat(literal(path), attrs);
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
        return !closed && !broken && session.isConnected() && channel.isConnected();
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
        // v3 stat replies carry numeric uid/gid; report them so ownership set through
        // setAttributes can be read back and verified rather than being write-only.
        boolean hasIds = (attrs.getFlags() & SftpATTRS.SSH_FILEXFER_ATTR_UIDGID) != 0;
        return new RemoteFile(
                name,
                path,
                attrs.isDir() ? 0 : Math.max(0, attrs.getSize()),
                attrs.isDir(),
                attrs.isLink(),
                // v3 mtimes are whole seconds, and JSch narrows them to a signed int: fine until
                // 2038, and there is nothing more precise on the wire to recover.
                Instant.ofEpochSecond(attrs.getMTime()),
                attrs.getPermissions() & 07777,
                hasIds ? attrs.getUId() : RemoteFile.UNKNOWN_ID,
                hasIds ? attrs.getGId() : RemoteFile.UNKNOWN_ID);
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

    /**
     * Carries JSch's {@code id} across as the SSH_FX status, so callers never parse messages — when it
     * really is one.
     *
     * <p>A JSch exception with a cause did not come from a server status reply: JSch wraps anything
     * that goes wrong mid-request as {@code SSH_FX_FAILURE}. Reporting that as a server status would
     * misdescribe it, and the channel may now be out of step, so the session marks itself broken and
     * the pool will not hand it out again.
     */
    private SftpException translate(String what, com.jcraft.jsch.SftpException e) {
        if (e.getCause() != null) {
            broken = true;
            return new SftpException(what + " failed: " + e.getMessage(), e);
        }
        return new SftpException(what + " failed: " + e.getMessage(), e, e.id);
    }

    /**
     * Escapes a path so JSch treats it literally rather than as a glob — the same escaping JSch's own
     * package-private {@code Util.quote} applies: a backslash before every {@code \}, {@code *} and
     * {@code ?}. JSch strips exactly one level of it again before the path goes on the wire.
     */
    static String literal(String path) {
        if (path.indexOf('\\') < 0 && path.indexOf('*') < 0 && path.indexOf('?') < 0) {
            return path;
        }
        StringBuilder escaped = new StringBuilder(path.length() + 8);
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '\\' || c == '*' || c == '?') {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }

    @FunctionalInterface
    private interface SftpOperation<T> {
        T run() throws com.jcraft.jsch.SftpException;
    }
}
