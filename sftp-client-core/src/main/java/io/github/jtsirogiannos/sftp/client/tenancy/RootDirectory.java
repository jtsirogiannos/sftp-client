package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.api.SftpPaths;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The arithmetic of a confined view: mapping paths in from the view's namespace to the server's, and
 * out again.
 *
 * <p>Inside the view, {@code /} is the root directory and every path is resolved against it —
 * relative paths too, because "relative to the login directory" would name a directory outside the
 * root. Going in, a path is normalised lexically and refused if it could name anything outside:
 *
 * <ul>
 *   <li>{@code ..} that climbs above {@code /};</li>
 *   <li>a backslash, which JSch has historically unescaped and Windows servers treat as a
 *       separator, so {@code .\.} could reach the server as {@code ..};</li>
 *   <li>NUL, which truncates a path in any C-string server;</li>
 *   <li>a leading {@code ~}, which some servers expand to the account's home — outside the root;</li>
 *   <li>the empty path, which names nothing.</li>
 * </ul>
 *
 * <p>Coming out, the root is stripped from paths the server reports. Listing and stat replies carry
 * the path as it was asked for, so they always start with the root as written. {@code realpath} and
 * absolute {@code readlink} answers are canonical, and if any directory in the root is a symlink they
 * start with its target instead — so those are also tried against the canonical root, resolved once
 * on first need. Anything that starts with neither is outside, and is refused rather than shown.
 *
 * <p>Refusals are {@link SftpException}s carrying {@code SSH_FX_PERMISSION_DENIED}: from the caller's
 * side that is exactly what happened, and {@code isPermissionDenied()} is how they would test for it.
 */
final class RootDirectory {

    private final String root;
    private volatile String canonical;

    private RootDirectory(String root) {
        this.root = root;
    }

    /**
     * @throws IllegalArgumentException for a relative root, one containing a character the view would
     *                                  refuse, or {@code /} itself, which confines nothing
     */
    static RootDirectory of(String rootDirectory) {
        Objects.requireNonNull(rootDirectory, "rootDirectory");
        if (rootDirectory.isEmpty() || rootDirectory.charAt(0) != '/') {
            throw new IllegalArgumentException(
                    "a root directory must be absolute, got '" + rootDirectory + "'");
        }
        if (rootDirectory.indexOf('\\') >= 0 || rootDirectory.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "a root directory must not contain a backslash or NUL: '" + rootDirectory + "'");
        }
        String normalised = SftpPaths.normalize(rootDirectory);
        if ("/".equals(normalised)) {
            throw new IllegalArgumentException("a root directory of '/' confines nothing");
        }
        return new RootDirectory(normalised);
    }

    /** The root as written, normalised. */
    String path() {
        return root;
    }

    /** A path in the view's namespace, as the server must be asked for it. */
    String toServer(String path) {
        Objects.requireNonNull(path, "path");
        if (path.isEmpty()) {
            throw refused(path, "an empty path names nothing");
        }
        if (path.indexOf('\\') >= 0) {
            throw refused(path, "a backslash may be read as an escape or a separator by the server");
        }
        if (path.indexOf('\0') >= 0) {
            throw refused(path, "it contains NUL");
        }
        if (path.charAt(0) == '~') {
            throw refused(path, "a server may expand '~' to the account's home directory");
        }
        List<String> segments = new ArrayList<>();
        for (String segment : path.split("/")) {
            if (segment.isEmpty() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                if (segments.isEmpty()) {
                    throw refused(path, "it climbs above the root");
                }
                segments.remove(segments.size() - 1);
                continue;
            }
            segments.add(segment);
        }
        return segments.isEmpty() ? root : root + "/" + String.join("/", segments);
    }

    /** The normalised form of a path in the view's own namespace — what {@link #toServer} would mean. */
    String normalise(String path) {
        return inView(toServer(path));
    }

    /**
     * A path the server reported, translated back into the view's namespace.
     *
     * @param via where to resolve the canonical root, should it be needed
     */
    String fromServer(String serverPath, SftpOperations via) {
        String inside = inView(serverPath);
        if (inside == null) {
            inside = strip(serverPath, canonical(via));
        }
        if (inside == null) {
            throw new SftpException("a path the server reported lies outside this client's root "
                    + "directory, so it is not shown", null, SftpException.SSH_FX_PERMISSION_DENIED);
        }
        return inside;
    }

    /**
     * {@link #fromServer(String, SftpOperations)} for the reactive facade: the canonical root, when it
     * is needed and not yet known, is resolved without blocking.
     */
    java.util.concurrent.CompletionStage<String> fromServerAsync(String serverPath,
            io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations via) {
        String inside = inView(serverPath);
        if (inside != null) {
            return java.util.concurrent.CompletableFuture.completedFuture(inside);
        }
        java.util.concurrent.CompletionStage<String> resolved = canonical != null
                ? java.util.concurrent.CompletableFuture.completedFuture(canonical)
                : via.realpath(root).thenApply(answer -> {
                    String normalised = SftpPaths.normalize(answer);
                    canonical = normalised;
                    return normalised;
                });
        return resolved.thenApply(canonicalRoot -> {
            String stripped = strip(serverPath, canonicalRoot);
            if (stripped == null) {
                throw new SftpException("a path the server reported lies outside this client's root "
                        + "directory, so it is not shown", null, SftpException.SSH_FX_PERMISSION_DENIED);
            }
            return stripped;
        });
    }

    private String inView(String serverPath) {
        return strip(serverPath, root);
    }

    private String canonical(SftpOperations via) {
        String resolved = canonical;
        if (resolved == null) {
            resolved = SftpPaths.normalize(via.realpath(root));
            canonical = resolved;
        }
        return resolved;
    }

    private static String strip(String serverPath, String prefix) {
        if (serverPath.equals(prefix)) {
            return "/";
        }
        if (serverPath.startsWith(prefix) && serverPath.charAt(prefix.length()) == '/') {
            return serverPath.substring(prefix.length());
        }
        return null;
    }

    private static SftpException refused(String path, String why) {
        return new SftpException("'" + path + "' is outside this client's root directory: " + why,
                null, SftpException.SSH_FX_PERMISSION_DENIED);
    }

    @Override
    public String toString() {
        return root;
    }
}
