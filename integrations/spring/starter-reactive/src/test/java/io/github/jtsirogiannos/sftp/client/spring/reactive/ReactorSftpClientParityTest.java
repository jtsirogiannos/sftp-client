package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every method on the neutral reactive types must exist on its Reactor counterpart by name — the
 * same tripwire the Kotlin and Mutiny wrappers carry, because all three are maintained by hand and
 * all three once silently lagged the neutral client. Name-level, since the wrapper's job is changing
 * the types. Checked type by type, so a method landing on the wrong one is caught too.
 */
class ReactorSftpClientParityTest {

    @Test
    void operationsCoverEveryNeutralOperationByName() {
        assertCovered(Arrays.asList(ReactiveSftpOperations.class.getMethods()), ReactorSftpOperations.class);
    }

    @Test
    void theClientCoversEveryNeutralClientMethodByName() {
        assertCovered(Arrays.asList(ReactiveSftpClient.class.getDeclaredMethods()), ReactorSftpClient.class);
    }

    @Test
    void theSessionCoversEveryNeutralSessionMethodByName() {
        assertCovered(Arrays.asList(ReactiveSftpClientSession.class.getDeclaredMethods()),
            ReactorSftpSession.class);
    }

    private static void assertCovered(List<Method> neutral, Class<?> wrapper) {
        Set<String> wrapped = Arrays.stream(wrapper.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        List<String> missing = neutral.stream()
            .filter(m -> !m.isSynthetic() && Modifier.isPublic(m.getModifiers()))
            .map(Method::getName)
            .distinct()
            .filter(name -> !wrapped.contains(name))
            .sorted()
            .toList();

        assertThat(missing)
            .describedAs("neutral methods with no Reactor counterpart on " + wrapper.getSimpleName()
                + "; each is a one-line delegation away")
            .isEmpty();
    }
}
