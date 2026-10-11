package io.github.jtsirogiannos.sftp.client.api;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Lexical arithmetic on remote paths, the way an SFTP server spells them: {@code /} separates, and
 * nothing else is special — not {@code \}, not {@code *}, not {@code ~}.
 *
 * <p><strong>Lexical</strong> means the server is never asked. {@code /a/link/..} normalises to
 * {@code /a} even when {@code link} is a symlink and the server would resolve it somewhere else. That
 * is the right tool for refusing an obviously wrong path before anything is sent, and the wrong tool
 * for deciding where a path really points — {@link SftpOperations#realpath} answers that.
 */
public final class SftpPaths {

    private SftpPaths() {
    }

    /**
     * Collapses repeated slashes, drops {@code .} segments and resolves {@code ..} against the
     * segment before it.
     *
     * <ul>
     *   <li>An absolute path stays absolute, and {@code ..} above {@code /} stays at {@code /}, as
     *       POSIX resolves it.</li>
     *   <li>A relative path stays relative — it is relative to the login directory, which only the
     *       server knows — keeping any leading {@code ..} it cannot resolve. One that resolves to
     *       nothing is {@code "."}.</li>
     *   <li>A trailing slash is dropped, except from {@code /} itself.</li>
     * </ul>
     *
     * @throws IllegalArgumentException if {@code path} is empty, which names nothing at all
     */
    public static String normalize(String path) {
        Objects.requireNonNull(path, "path");
        if (path.isEmpty()) {
            throw new IllegalArgumentException("an empty remote path names nothing");
        }
        boolean absolute = path.charAt(0) == '/';
        Deque<String> segments = new ArrayDeque<>();
        for (String segment : path.split("/")) {
            if (segment.isEmpty() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                if (!segments.isEmpty() && !"..".equals(segments.peekLast())) {
                    segments.pollLast();
                } else if (!absolute) {
                    segments.addLast(segment);
                }
                // Absolute and already at the root: /.. is /.
                continue;
            }
            segments.addLast(segment);
        }
        String joined = String.join("/", segments);
        if (absolute) {
            return "/" + joined;
        }
        return joined.isEmpty() ? "." : joined;
    }

    /**
     * Why {@code deleteRecursively} must refuse {@code path}, or null when it need not.
     *
     * <p>Shared by the blocking and reactive facades so they refuse exactly the same things. Checked on
     * the normalised form: comparing the raw string against {@code "/"} let {@code "/."},
     * {@code "//"} and {@code "/upload/.."} straight through, and a relative path like {@code "a/.."}
     * resolves to the login directory, which is the account's whole world on most servers.
     */
    static SftpException refusalToDeleteRecursively(String path) {
        String normalised;
        try {
            normalised = normalize(path);
        } catch (IllegalArgumentException empty) {
            return new SftpException("refusing to delete an empty path");
        }
        if ("/".equals(normalised)) {
            return new SftpException("refusing to delete the remote root directory"
                + ("/".equals(path) ? "" : ": '" + path + "' resolves to it"));
        }
        if (".".equals(normalised) || normalised.matches("\\.\\.(/\\.\\.)*")) {
            return new SftpException("refusing to delete '" + path
                + "': it resolves to the login directory or one of its ancestors");
        }
        return null;
    }
}
