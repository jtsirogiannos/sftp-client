package io.github.jtsirogiannos.sftp.client.samples;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Asserts that every Java line in the root README is present in {@link ReadmeSamples}. Compilation of
 * that mirror already proves the API it names is real; this proves the mirror still matches the
 * README, so a snippet edited without the mirror — or a mirror quietly relaxed to keep compiling —
 * fails the build.
 *
 * <p>The comparison is line by line on a whitespace-collapsed form, split on the {@code ...}
 * placeholders the README uses so {@code blocking(...) { ... }} matches a real body. Two kinds of
 * block are skipped on purpose, and the skip is asserted rather than silent: the {@code @Inject
 * @NamedSftp} and {@code @Qualifier} field fragments, which would pull Quarkus and Spring onto this
 * classpath for two annotations. Kotlin blocks are not {@code java} fences and never reach here; the
 * Kotlin module's parity test covers them.
 */
class ReadmeSamplesInSyncTest {

    @Test
    void everyReadmeJavaLineIsMirrored() throws IOException {
        String readme = Files.readString(locate("README.md",
                Path.of("..", "README.md"), Path.of("README.md")));
        String mirror = collapse(Files.readString(locate("ReadmeSamples.java",
                Path.of("src", "test", "java", "io", "github", "jtsirogiannos", "sftp", "client",
                        "samples", "ReadmeSamples.java"))));

        List<List<String>> javaBlocks = fencedBlocks(readme, "java");
        assertTrue(javaBlocks.size() >= 8,
                "expected the README to still carry the Java samples; found " + javaBlocks.size());

        int skipped = 0;
        List<String> misses = new ArrayList<>();
        for (List<String> block : javaBlocks) {
            String joined = String.join("\n", block);
            if (joined.contains("@NamedSftp") || joined.contains("@Qualifier")) {
                skipped++; // framework field fragment — deliberately not mirrored
                continue;
            }
            for (String rawLine : block) {
                for (String fragment : significantFragments(rawLine)) {
                    if (!mirror.contains(fragment)) {
                        misses.add(fragment);
                    }
                }
            }
        }

        assertTrue(skipped <= 2,
                "more skipped Java blocks than the two known framework fragments (" + skipped
                        + "); a new snippet may be silently uncovered");
        if (!misses.isEmpty()) {
            fail("README Java lines missing from ReadmeSamples (update the mirror, verbatim):\n  "
                    + String.join("\n  ", misses));
        }
    }

    /** Fenced ``` blocks of the given language, each as its list of raw lines. */
    private static List<List<String>> fencedBlocks(String text, String language) {
        List<List<String>> blocks = new ArrayList<>();
        String open = "```" + language;
        List<String> current = null;
        for (String line : text.split("\n", -1)) {
            String trimmed = line.strip();
            if (current == null) {
                if (trimmed.equals(open)) {
                    current = new ArrayList<>();
                }
            } else if (trimmed.equals("```")) {
                blocks.add(current);
                current = null;
            } else {
                current.add(line);
            }
        }
        return blocks;
    }

    /**
     * The parts of a README line that must appear in the mirror: comment stripped, split on the
     * {@code ...} placeholder, each part whitespace-collapsed, with bare punctuation ({@code }},
     * {@code });}) dropped as noise that would match anywhere.
     */
    private static List<String> significantFragments(String rawLine) {
        String line = rawLine;
        int comment = line.indexOf("//");
        if (comment >= 0) {
            line = line.substring(0, comment);
        }
        List<String> fragments = new ArrayList<>();
        for (String part : line.split("\\.\\.\\.")) {
            String fragment = collapse(part);
            if (fragment.length() >= 4 && !fragment.matches("[{}();,\\s]+")) {
                fragments.add(fragment);
            }
        }
        return fragments;
    }

    private static String collapse(String s) {
        return s.replaceAll("\\s+", " ").strip();
    }

    /** Resolve a file against the module basedir (surefire's working directory), trying candidates. */
    private static Path locate(String label, Path... candidates) {
        for (Path candidate : candidates) {
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("could not locate " + label + " from "
                + Path.of("").toAbsolutePath() + "; tried " + List.of(candidates));
    }
}
