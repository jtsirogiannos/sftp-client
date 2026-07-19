package io.github.jtsirogiannos.sftp.client.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

/**
 * The conformance suite every provider must pass. Extend it once per provider:
 *
 * <pre>{@code
 * class MinaConformanceTest extends SftpProviderConformanceTest {
 *     protected SftpProvider provider() { return new MinaSftpProvider(); }
 * }
 * }</pre>
 *
 * <p>This class is what makes the SPI real rather than decorative — the same assertions run against
 * JSch, MINA, and the in-memory fake. If a test can only pass for one backend, the contract is
 * underspecified, not the test.
 *
 * <p>Run it against an in-process Apache MINA SSHD server (see {@link EmbeddedSftpServer}) so no
 * Docker is needed — including for the JSch provider.
 */
public abstract class SftpProviderConformanceTest {

    /** Supply the provider under test. */
    protected abstract SftpProvider provider();

    /** Open a session against the fixture server. */
    protected abstract SftpSession session();

    // --- metadata ---

    @Test
    void listSkipsDotEntries() {
        session().mkdir("/upload/sub");
        List<RemoteFile> entries = session().list("/upload");
        assertThat(entries).extracting(RemoteFile::name).doesNotContain(".", "..");
    }

    /**
     * A listed entry's {@code path()} must be usable as-is on the next call.
     *
     * <p>Anything that walks a tree — {@code SftpClient.deleteRecursively} above all — feeds the
     * path from a listing straight back into {@code stat} or {@code remove}. A provider that
     * returned a bare filename, or joined with a doubled slash, would break traversal while every
     * single-path test kept passing.
     */
    @Test
    void listedEntriesCarryAUsableAbsolutePath() {
        session().mkdir("/upload/paths");
        writeFile("/upload/paths/entry.txt", "x");

        RemoteFile entry = session().list("/upload/paths").stream()
                .filter(f -> f.name().equals("entry.txt"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the entry just written was not listed"));

        assertThat(entry.path()).isEqualTo("/upload/paths/entry.txt");
        assertThat(session().stat(entry.path()).size())
                .describedAs("the listed path must work when fed straight back in")
                .isEqualTo(1);
    }

    // --- streaming a listing ---

    /** The visitor must see exactly what the collected listing does, dot entries excluded. */
    @Test
    void visitorSeesEveryEntry() {
        session().mkdir("/upload/walk");
        writeFile("/upload/walk/a.txt", "a");
        writeFile("/upload/walk/b.txt", "b");
        writeFile("/upload/walk/c.txt", "c");

        List<String> visited = new ArrayList<>();
        session().list("/upload/walk", entry -> {
            visited.add(entry.name());
            return true;
        });

        assertThat(visited).containsExactlyInAnyOrder("a.txt", "b.txt", "c.txt");
        assertThat(visited).doesNotContain(".", "..");
    }

    /**
     * Returning false must actually stop the walk, not merely filter what the caller is shown.
     *
     * <p>The point of the whole streaming API: finding one file in a large directory should not mean
     * paging through the rest of it.
     */
    @Test
    void visitorStopsTheWalkWhenItReturnsFalse() {
        session().mkdir("/upload/stop");
        for (int i = 0; i < 20; i++) {
            writeFile("/upload/stop/file-" + i + ".txt", "x");
        }

        AtomicInteger visited = new AtomicInteger();
        session().list("/upload/stop", entry -> {
            visited.incrementAndGet();
            return false; // stop at the first one
        });

        assertThat(visited.get())
                .describedAs("the walk continued after the visitor asked it to stop")
                .isEqualTo(1);
    }

    /**
     * Stopping early leaves entries queued on the wire; the provider must close the directory handle
     * and leave the session able to do something else.
     */
    @Test
    void sessionRemainsUsableAfterAnAbandonedWalk() {
        session().mkdir("/upload/abandon-walk");
        for (int i = 0; i < 20; i++) {
            writeFile("/upload/abandon-walk/file-" + i + ".txt", "x");
        }

        session().list("/upload/abandon-walk", entry -> false);

        assertThat(session().isValid()).isTrue();
        assertThat(session().stat("/upload/abandon-walk").directory())
                .describedAs("the session must still work after a walk was abandoned")
                .isTrue();
    }

    @Test
    void statReportsSizeAndDirectoryFlag() {
        writeFile("/upload/a.txt", "hello");
        assertThat(session().stat("/upload/a.txt").size()).isEqualTo(5);
        assertThat(session().stat("/upload/a.txt").directory()).isFalse();
    }

    // --- realpath, attributes, extensions ---

    /**
     * {@code realpath} is how a caller discovers where it actually is — every other method assumes
     * an absolute path it has no way to obtain.
     */
    @Test
    void realpathCanonicalisesAndResolvesTheStartDirectory() {
        writeFile("/upload/real.txt", "x");

        assertThat(session().realpath("/upload/./real.txt")).isEqualTo("/upload/real.txt");
        assertThat(session().realpath("/upload/sub/../real.txt")).isEqualTo("/upload/real.txt");
        assertThat(session().realpath("."))
                .describedAs("realpath(\".\") must name the session's starting directory")
                .startsWith("/");
    }

    @Test
    void setAttributesChangesPermissions() {
        writeFile("/upload/mode.txt", "x");

        session().setAttributes("/upload/mode.txt",
                AttributeUpdate.builder().permissions(0600).build());

        assertThat(session().stat("/upload/mode.txt").permissions() & 0777)
                .describedAs("the mode the server reports back must be the one we set")
                .isEqualTo(0600);
    }

    /** Only the fields present in the update may be touched; the rest are the server's business. */
    @Test
    void setAttributesLeavesUnsetFieldsAlone() {
        writeFile("/upload/partial.txt", "0123456789");
        long sizeBefore = session().stat("/upload/partial.txt").size();

        session().setAttributes("/upload/partial.txt",
                AttributeUpdate.builder().permissions(0640).build());

        assertThat(session().stat("/upload/partial.txt").size())
                .describedAs("a permissions-only update must not resize the file")
                .isEqualTo(sizeBefore);
    }

    /** Setting a smaller size truncates — the protocol's only truncate. */
    @Test
    void setAttributesTruncates() {
        writeFile("/upload/truncate-me.txt", "0123456789");

        session().setAttributes("/upload/truncate-me.txt",
                AttributeUpdate.builder().size(4).build());

        assertThat(session().stat("/upload/truncate-me.txt").size()).isEqualTo(4);
    }

    @Test
    void setAttributesWithNothingSetIsANoOp() {
        writeFile("/upload/untouched.txt", "abc");
        session().setAttributes("/upload/untouched.txt", AttributeUpdate.builder().build());
        assertThat(session().stat("/upload/untouched.txt").size()).isEqualTo(3);
    }

    /** Resuming a download: the protocol has carried a read offset since v3. */
    @Test
    void openReadHonoursAnOffset() throws Exception {
        writeFile("/upload/offset.txt", "0123456789");

        try (InputStream in = session().openRead("/upload/offset.txt", 6)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("6789");
        }
        try (InputStream in = session().openRead("/upload/offset.txt", 100)) {
            assertThat(in.readAllBytes())
                    .describedAs("an offset past the end should be empty, not an error")
                    .isEmpty();
        }
    }

    /**
     * Hard links and statvfs are OpenSSH extensions, not part of any SFTP draft, so the capability
     * decides. A session that declines must decline cleanly rather than fail obscurely.
     */
    @Test
    void hardLinksFollowTheCapability() {
        if (!session().capabilities().hardLinks()) {
            assertThatThrownBy(() -> session().hardlink("/upload/hard.link", "/upload/target.txt"))
                    .isInstanceOf(UnsupportedOperationException.class);
            return;
        }
        writeFile("/upload/hard-target.txt", "shared content");

        session().hardlink("/upload/hard.link", "/upload/hard-target.txt");

        assertThat(session().stat("/upload/hard.link").size()).isEqualTo(14);
        assertThat(session().lstat("/upload/hard.link").symlink())
                .describedAs("a hard link is not a symlink")
                .isFalse();
    }

    @Test
    void statvfsFollowsTheCapability() {
        if (!session().capabilities().statvfs()) {
            assertThatThrownBy(() -> session().statvfs("/upload"))
                    .isInstanceOf(UnsupportedOperationException.class);
            return;
        }
        var stats = session().statvfs("/upload");

        assertThat(stats.totalBytes()).isPositive();
        assertThat(stats.availableBytes()).isLessThanOrEqualTo(stats.freeBytes());
    }

    // --- symlinks ---

    /**
     * Creates a link and reads it back, which pins down the argument order.
     *
     * <p>Worth a dedicated test because the backends disagree: MINA takes {@code (link, target)} and
     * JSch takes {@code (target, link)}. A provider that forwards them unchanged produces a link
     * with the two swapped — and since both paths exist in these tests, a swapped implementation
     * still "works" until something reads the link or follows it.
     */
    @Test
    void createsAndReadsBackASymlink() {
        if (!session().capabilities().symlinks()) {
            assertThatThrownBy(() -> session().symlink("/upload/link", "/upload/target.txt"))
                    .isInstanceOf(UnsupportedOperationException.class);
            return;
        }
        writeFile("/upload/target.txt", "pointed at");

        session().symlink("/upload/link.txt", "/upload/target.txt");

        assertThat(session().readlink("/upload/link.txt"))
                .describedAs("readlink must return the target, not the link")
                .isEqualTo("/upload/target.txt");
    }

    /** stat follows a link and reports the target; lstat reports the link itself. */
    @Test
    void lstatSeesTheLinkWhereStatSeesTheTarget() {
        if (!session().capabilities().symlinks()) {
            return;
        }
        writeFile("/upload/stat-target.txt", "0123456789");
        session().symlink("/upload/stat-link.txt", "/upload/stat-target.txt");

        RemoteFile followed = session().stat("/upload/stat-link.txt");
        RemoteFile link = session().lstat("/upload/stat-link.txt");

        assertThat(followed.symlink())
                .describedAs("stat follows the link, so it cannot report one")
                .isFalse();
        assertThat(followed.size())
                .describedAs("stat should report the target's size")
                .isEqualTo(10);
        assertThat(link.symlink())
                .describedAs("lstat must report the link itself — the check every tree walk depends on")
                .isTrue();
    }

    /** Removing a link must not touch what it points at. */
    @Test
    void removingASymlinkLeavesTheTargetAlone() {
        if (!session().capabilities().symlinks()) {
            return;
        }
        writeFile("/upload/keep-me.txt", "still here");
        session().symlink("/upload/doomed-link.txt", "/upload/keep-me.txt");

        session().remove("/upload/doomed-link.txt");

        assertThat(session().exists("/upload/doomed-link.txt")).isFalse();
        assertThat(session().exists("/upload/keep-me.txt"))
                .describedAs("removing a link must never delete its target")
                .isTrue();
    }

    @Test
    void existsIsFalseForMissingPath() {
        assertThat(session().exists("/upload/definitely-not-here")).isFalse();
    }

    // --- transfer ---

    @Test
    void roundTripsLargeFile() throws Exception {
        // Large on purpose: it's the only way to exercise multi-chunk reads and pipelining. A
        // pseudo-random payload rather than zeros, so a provider that drops or duplicates a chunk
        // fails instead of silently comparing one run of identical bytes against another.
        byte[] payload = new byte[8 * 1024 * 1024];
        new Random(20260718L).nextBytes(payload);

        try (OutputStream out = session().openWrite("/upload/large.bin", SftpSession.WriteMode.OVERWRITE)) {
            out.write(payload);
        }

        assertThat(session().stat("/upload/large.bin").size()).isEqualTo(payload.length);

        byte[] readBack;
        try (InputStream in = session().openRead("/upload/large.bin")) {
            readBack = in.readAllBytes();
        }
        // Compare with isEqualTo, not element-by-element assertions: on an 8 MiB mismatch the
        // latter produces a diff no one can read.
        assertThat(readBack).isEqualTo(payload);
    }

    @Test
    void readingPastEndOfFileSignalsEof() throws Exception {
        writeFile("/upload/eof.txt", "12345");
        try (InputStream in = session().openRead("/upload/eof.txt")) {
            assertThat(in.readAllBytes()).hasSize(5);
            // A provider that maps SSH_FX_EOF to an exception instead of -1 breaks every
            // InputStream consumer in the JDK, including the core's publisher.
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void appendAddsToExistingFile() throws Exception {
        writeFile("/upload/append.txt", "abc");
        try (OutputStream out = session().openWrite("/upload/append.txt", SftpSession.WriteMode.APPEND)) {
            out.write("def".getBytes(StandardCharsets.UTF_8));
        }
        try (InputStream in = session().openRead("/upload/append.txt")) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("abcdef");
        }
    }

    @Test
    void overwriteTruncatesExistingFile() throws Exception {
        writeFile("/upload/truncate.txt", "long original content");
        writeFile("/upload/truncate.txt", "short");
        // Without SSH_FXF_TRUNC the tail of the old content survives past the new bytes.
        assertThat(session().stat("/upload/truncate.txt").size()).isEqualTo(5);
    }

    @Test
    void renameMovesFile() throws Exception {
        writeFile("/upload/before.txt", "x");
        session().rename("/upload/before.txt", "/upload/after.txt");
        assertThat(session().exists("/upload/before.txt")).isFalse();
        assertThat(session().exists("/upload/after.txt")).isTrue();
    }

    /**
     * The contract is "a file, <em>or an empty directory</em>", and the second half had no test.
     *
     * <p>It is a separate protocol request — {@code SSH_FXP_RMDIR} rather than {@code
     * SSH_FXP_REMOVE} — and both providers originally issued only the file one, so removing a
     * directory failed against every server. Nothing caught it: every other test removes files, and
     * an in-memory fake will happily delete whatever you ask it to.
     */
    @Test
    void removeDeletesAnEmptyDirectory() {
        session().mkdir("/upload/empty-dir");
        assertThat(session().exists("/upload/empty-dir")).isTrue();

        session().remove("/upload/empty-dir");

        assertThat(session().exists("/upload/empty-dir")).isFalse();
    }

    @Test
    void removeDeletesFile() throws Exception {
        writeFile("/upload/doomed.txt", "x");
        session().remove("/upload/doomed.txt");
        assertThat(session().exists("/upload/doomed.txt")).isFalse();
    }

    /**
     * The capability model earning its keep. JSch's client API cannot send {@code SSH_FXF_EXCL}, so
     * it reports {@code atomicCreateNew = false} and refuses loudly; MINA can, so it must fail the
     * write. What no provider may do is quietly stat-then-create, which passes a naive version of
     * this test and loses data under concurrency — so each branch asserts a different, specific
     * failure rather than just "something threw".
     */
    @Test
    void createNewFailsWhenPathExists() {
        writeFile("/upload/dup.txt", "x");

        if (!session().capabilities().atomicCreateNew()) {
            assertThatThrownBy(() -> session().openWrite("/upload/dup.txt", SftpSession.WriteMode.CREATE_NEW))
                    .describedAs("a provider without atomicCreateNew must refuse, not emulate")
                    .isInstanceOf(UnsupportedOperationException.class);
            return;
        }

        assertThatThrownBy(() -> session().openWrite("/upload/dup.txt", SftpSession.WriteMode.CREATE_NEW))
                .isInstanceOf(SftpException.class);
    }

    @Test
    void createNewSucceedsOnFreshPath() throws Exception {
        if (!session().capabilities().atomicCreateNew()) {
            return; // covered by the refusal branch above
        }
        try (OutputStream out = session().openWrite("/upload/fresh.txt", SftpSession.WriteMode.CREATE_NEW)) {
            out.write("new".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(session().stat("/upload/fresh.txt").size()).isEqualTo(3);
    }

    /**
     * Renaming onto an existing path.
     *
     * <p>With {@code posixRename} the replacement is guaranteed. Without it the outcome belongs to
     * the <em>server</em>, not the client: OpenSSH refuses, MINA SSHD's server happily replaces, and
     * the JSch provider therefore "succeeds" here despite reporting {@code posixRename = false}.
     * Asserting a specific outcome in that branch would encode one server's behaviour as the
     * contract, and fail against the other.
     *
     * <p>So the suite pins the invariant that is genuinely specifiable and worth protecting: however
     * it turns out, the pair is never left with data missing. A provider that removes the source and
     * then fails to place the destination still fails this test.
     */
    @Test
    void renameOntoExistingPathNeverLosesData() {
        writeFile("/upload/src.txt", "source");
        writeFile("/upload/dst.txt", "destination");

        if (session().capabilities().posixRename()) {
            session().rename("/upload/src.txt", "/upload/dst.txt");
            assertThat(session().exists("/upload/src.txt")).isFalse();
            assertThat(session().stat("/upload/dst.txt").size()).isEqualTo(6);
            return;
        }

        try {
            session().rename("/upload/src.txt", "/upload/dst.txt");
            // Replaced: the source is consumed and the destination holds the source bytes.
            assertThat(session().exists("/upload/src.txt")).isFalse();
            assertThat(session().stat("/upload/dst.txt").size()).isEqualTo(6);
        } catch (SftpException refused) {
            // Refused: nothing moved, and both files are exactly as they were.
            assertThat(session().stat("/upload/src.txt").size()).isEqualTo(6);
            assertThat(session().stat("/upload/dst.txt").size()).isEqualTo(11);
        }
    }

    // --- error mapping ---

    @Test
    void providerFailuresSurfaceAsSftpException() {
        assertThatThrownBy(() -> session().stat("/nope/missing"))
                .isInstanceOf(SftpException.class);
    }

    @Test
    void sessionRemainsUsableAfterRecoverableFailure() {
        assertThatThrownBy(() -> session().stat("/nope/missing")).isInstanceOf(SftpException.class);
        assertThat(session().isValid()).isTrue();
    }

    // --- capability honesty ---

    @Test
    void capabilitiesAreSelfConsistent() {
        assertThat(provider().capabilities().maxSftpVersion()).isBetween(3, 6);
        assertThat(session().capabilities().maxSftpVersion()).isBetween(3, 6);
    }

    /**
     * A session may only ever offer less than its provider advertises, never more. The provider
     * describes a best case it cannot verify; the session describes what this server agreed to. A
     * provider claiming v3 whose session reports v6 has its capability model backwards.
     */
    @Test
    void sessionNeverClaimsMoreThanTheProvider() {
        var provider = provider().capabilities();
        var negotiated = session().capabilities();

        assertThat(negotiated.maxSftpVersion()).isLessThanOrEqualTo(provider.maxSftpVersion());
        if (!provider.posixRename()) {
            assertThat(negotiated.posixRename()).isFalse();
        }
        if (!provider.atomicCreateNew()) {
            assertThat(negotiated.atomicCreateNew()).isFalse();
        }
    }

    /**
     * A read abandoned far from EOF must close cleanly and leave the session working.
     *
     * <p>This is the provider's half of the cancellation contract. When a subscriber cancels a
     * download, the core closes the stream partway through and discards the session — but "discard"
     * still means calling {@code close()} on a channel with unread bytes queued behind it. A
     * provider that hangs or throws there turns a routine cancellation into a stuck worker thread.
     *
     * <p>The demand accounting and pool-slot accounting that sit above this live in the core's
     * {@code ReactiveCancellationTest}, where they can be observed without a server.
     */
    @Test
    void abandoningAReadMidTransferIsClean() throws Exception {
        byte[] payload = new byte[2 * 1024 * 1024];
        new Random(4242L).nextBytes(payload);
        try (OutputStream out = session().openWrite("/upload/abandoned.bin",
                SftpSession.WriteMode.OVERWRITE)) {
            out.write(payload);
        }

        InputStream in = session().openRead("/upload/abandoned.bin");
        assertThat(in.read(new byte[4096]))
                .describedAs("expected to read the first chunk before abandoning")
                .isPositive();
        in.close(); // megabytes still unread on the wire

        assertThat(session().stat("/upload/abandoned.bin").size())
                .describedAs("the session must survive an abandoned read")
                .isEqualTo(payload.length);
    }

    private void writeFile(String path, String content) {
        try (OutputStream out = session().openWrite(path, SftpSession.WriteMode.OVERWRITE)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
