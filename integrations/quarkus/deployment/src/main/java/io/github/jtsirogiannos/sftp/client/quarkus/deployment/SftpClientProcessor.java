package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.NamedSftp;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.SftpClientRecorder;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.WithSftpSessionInterceptor;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;

import io.quarkus.arc.BeanDestroyer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.InterceptorBindingRegistrarBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.ValidationPhaseBuildItem;
import io.quarkus.arc.processor.InterceptorBindingRegistrar;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ServiceProviderBuildItem;
import io.quarkus.runtime.configuration.ConfigurationException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Turns {@code sftp.endpoints.*} into CDI beans: a blocking {@link SftpClient} and a
 * {@link MutinySftpClient} per endpoint, each qualified with {@link NamedSftp}.
 *
 * <p>They are synthetic beans because how many exist depends on configuration, so no amount of
 * annotation scanning would find them.
 *
 * <p><strong>Why endpoint names are read straight from {@link Config} here</strong> rather than
 * through a build-time {@code @ConfigMapping}. The obvious design is two mappings sharing the
 * {@code sftp} prefix — one build-time for the names, one run-time for hosts and credentials, the
 * way Quarkus splits its own datasource config. It does not work: a build-time root that claims the
 * prefix makes SmallRye validate <em>every</em> {@code sftp.*} property during augmentation, and the
 * run-time half is not visible in that phase, so a perfectly valid {@code sftp.endpoints.x.host}
 * fails the build with "does not map to any root". Reading the raw config avoids claiming the prefix
 * at build time, and leaves one mapping as the single description of the configuration.
 */
class SftpClientProcessor {

    private static final String FEATURE = "sftp-client";
    private static final String ENDPOINT_PREFIX = "sftp.endpoints.";
    private static final String DEFAULT_PROVIDER = "mina";
    private static final DotName DEFAULT_QUALIFIER =
            DotName.createSimple("jakarta.enterprise.inject.Default");

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * Makes {@link WithSftpSession} a CDI interceptor binding. It lives in the dependency-free API,
     * where it cannot be annotated as one; {@code value} is non-binding, so the one interceptor
     * serves every endpoint and reads the name from the binding at run time.
     */
    @BuildStep
    InterceptorBindingRegistrarBuildItem withSftpSessionBinding() {
        return new InterceptorBindingRegistrarBuildItem(new InterceptorBindingRegistrar() {
            @Override
            public List<InterceptorBinding> getAdditionalBindings() {
                return List.of(InterceptorBinding.of(WithSftpSession.class, Set.of("value")));
            }
        });
    }

    @BuildStep
    AdditionalBeanBuildItem withSftpSessionInterceptor() {
        return AdditionalBeanBuildItem.unremovableOf(WithSftpSessionInterceptor.class);
    }

    /**
     * Refuses a {@link WithSftpSession} the application could not honour, failing the build rather
     * than the first request: an endpoint that is not configured, an empty value when there is not
     * exactly one endpoint, or a return type that completes after the method returns without being a
     * {@code Uni} — the one asynchronous type the interceptor can keep a session open across.
     */
    @BuildStep
    void validateWithSftpSession(ValidationPhaseBuildItem validationPhase, CombinedIndexBuildItem index,
            BuildProducer<ValidationPhaseBuildItem.ValidationErrorBuildItem> errors) {
        Set<String> endpoints = discoverEndpoints().keySet();
        List<String> problems = new ArrayList<>();
        for (AnnotationInstance annotation : index.getIndex().getAnnotations(WITH_SFTP_SESSION)) {
            AnnotationValue value = annotation.value();
            String endpoint = value == null ? "" : value.asString();
            List<MethodInfo> methods = new ArrayList<>();
            if (annotation.target().kind() == AnnotationTarget.Kind.METHOD) {
                methods.add(annotation.target().asMethod());
            } else if (annotation.target().kind() == AnnotationTarget.Kind.CLASS) {
                ClassInfo type = annotation.target().asClass();
                if (type.name().equals(INTERCEPTOR)) {
                    continue; // the interceptor's own binding, not a use of it
                }
                for (MethodInfo method : type.methods()) {
                    if (Modifier.isPublic(method.flags()) && !Modifier.isStatic(method.flags())
                            && !method.isConstructor() && !method.isStaticInitializer()) {
                        methods.add(method);
                    }
                }
            } else {
                continue;
            }
            for (MethodInfo method : methods) {
                String where = method.declaringClass().name() + "." + method.name() + "()";
                if (endpoint.isEmpty() && endpoints.size() != 1) {
                    problems.add(where + " names no endpoint, and " + endpoints.size()
                            + " are configured " + endpoints + "; say which, as @WithSftpSession(\"name\")");
                } else if (!endpoint.isEmpty() && !endpoints.contains(endpoint)) {
                    problems.add(where + " names endpoint '" + endpoint + "', which is not configured; "
                            + "configured: " + endpoints);
                }
                String unsupported = unsupportedReturn(method);
                if (unsupported != null) {
                    problems.add(where + " " + unsupported);
                }
            }
        }
        if (!problems.isEmpty()) {
            errors.produce(new ValidationPhaseBuildItem.ValidationErrorBuildItem(new DeploymentException(
                    "@WithSftpSession cannot be honoured:" + System.lineSeparator() + "  "
                            + String.join(System.lineSeparator() + "  ", problems))));
        }
    }

    private static String unsupportedReturn(MethodInfo method) {
        List<org.jboss.jandex.Type> parameters = method.parameterTypes();
        if (!parameters.isEmpty()
                && parameters.get(parameters.size() - 1).name().toString().equals("kotlin.coroutines.Continuation")) {
            return "is a Kotlin suspend function; use SuspendingSftpClient.withSession { } inside it";
        }
        String returned = method.returnType().name().toString();
        if (UNSUPPORTED_RETURNS.contains(returned)) {
            return "returns " + returned + ", which completes after the method returns and is not a "
                    + "Uni; return Uni, or use the client's withSession";
        }
        return null;
    }

    private static final DotName WITH_SFTP_SESSION = DotName.createSimple(WithSftpSession.class.getName());
    private static final DotName INTERCEPTOR = DotName.createSimple(WithSftpSessionInterceptor.class.getName());
    private static final Set<String> UNSUPPORTED_RETURNS = Set.of(
            "io.smallrye.mutiny.Multi",
            "java.util.concurrent.CompletionStage",
            "java.util.concurrent.CompletableFuture",
            "java.util.concurrent.Flow$Publisher",
            "org.reactivestreams.Publisher");

    /**
     * Native image drops {@code META-INF/services} unless told otherwise, and provider lookup is
     * nothing but a {@link ServiceLoader} call — so without this the JVM build works and the native
     * one fails at first use with "No SFTP provider named ...". Reading from the classpath keeps it
     * automatic for third-party providers too.
     */
    @BuildStep
    ServiceProviderBuildItem registerProvidersForNativeImage() {
        return ServiceProviderBuildItem.allProvidersFromClassPath(SftpProvider.class.getName());
    }

    /**
     * Defers MINA's global {@code SecureRandom} to run time.
     *
     * <p>{@code JceRandom} caches a {@code SecureRandom} in a static field, so image generation
     * captures a seeded NativePRNG into the heap and GraalVM aborts the build — rightly: a baked-in
     * random source inside an SSH client would mean every deployed binary sharing predictable
     * randomness. Marking the class for run-time initialisation makes it seed on first use instead.
     *
     * <p>Guarded by a class-presence check rather than declared unconditionally, so this
     * provider-neutral extension does not force MINA onto a build that only uses JSch.
     *
     * <p>By the layout in the README this belongs to the mina bundle, which is meant to inherit its
     * native configuration from {@code quarkus-sshd}. It cannot: the bundle is deliberately
     * code-free, and quarkus-sshd — which targets the server side — does not register this. Left
     * here with the check, this is the smallest thing that makes a native client work at all.
     */
    @BuildStep
    void deferMinaRandomToRuntime(BuildProducer<RuntimeInitializedClassBuildItem> runtimeInitialized) {
        String jceRandom = "org.apache.sshd.common.random.JceRandom";
        if (!isPresent(jceRandom)) {
            return;
        }
        runtimeInitialized.produce(new RuntimeInitializedClassBuildItem(jceRandom));
        // The nested holder is the one that matters, and registering only the outer class is a
        // silent no-op: JceRandom$Cache has its own class initializer, which is what constructs the
        // SecureRandom. GraalVM's --trace-object-instantiation names it; nothing else does.
        runtimeInitialized.produce(new RuntimeInitializedClassBuildItem(jceRandom + "$Cache"));
    }

    private static boolean isPresent(String className) {
        try {
            Class.forName(className, false, Thread.currentThread().getContextClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Fails the build when an endpoint names a provider that is not on the classpath.
     *
     * <p>ROADMAP Phase 3 asks for exactly this: a missing driver should be a build failure, not a
     * surprise on the first request in production. The message names the endpoint and the artifact
     * to add, because that is the line someone reads while trying to fix it.
     */
    private static void validateConfiguredProviders(Map<String, String> endpoints) {
        if (endpoints.isEmpty()) {
            return;
        }
        var available = new TreeSet<String>();
        for (SftpProvider provider : ServiceLoader.load(SftpProvider.class,
                Thread.currentThread().getContextClassLoader())) {
            available.add(provider.name());
        }
        List<String> problems = new ArrayList<>();
        endpoints.forEach((name, provider) -> {
            if (!available.contains(provider)) {
                problems.add(ENDPOINT_PREFIX + name + ".provider=" + provider
                        + " but no such provider is on the classpath. Available: " + available
                        + ". Add the quarkus-sftp-client-" + provider + " dependency.");
            }
        });
        if (!problems.isEmpty()) {
            throw new ConfigurationException(String.join(System.lineSeparator(), problems));
        }
    }

    /**
     * Registers the beans. Runtime-init rather than static-init: building a client reads run-time
     * config, and a native image must never capture a host or a credential into the binary.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerClients(SftpClientRecorder recorder,
                         ExecutorBuildItem executor,
                         BuildProducer<SyntheticBeanBuildItem> syntheticBeans) {

        Map<String, String> endpoints = discoverEndpoints();
        // Validated here rather than in a build step of its own: a void build step that produces
        // nothing is pruned and never runs, so the check would silently do nothing.
        validateConfiguredProviders(endpoints);

        // With a single endpoint the beans are also @Default, so `@Inject SftpClient` works with no
        // qualifier - the same courtesy the Spring starter extends by marking its beans primary.
        boolean single = endpoints.size() == 1;

        endpoints.forEach((name, provider) -> {
            var blocking = SyntheticBeanBuildItem.configure(SftpClient.class)
                    .scope(ApplicationScoped.class)
                    .addQualifier(namedSftp(name))
                    .setRuntimeInit()
                    .unremovable()
                    .createWith(recorder.blockingClient(name, provider))
                    .destroyer(BeanDestroyer.AutoCloseableDestroyer.class);
            if (single) {
                blocking.addQualifier(DEFAULT_QUALIFIER);
            }
            syntheticBeans.produce(blocking.done());

            var mutiny = SyntheticBeanBuildItem.configure(MutinySftpClient.class)
                    .scope(ApplicationScoped.class)
                    .addQualifier(namedSftp(name))
                    .setRuntimeInit()
                    .unremovable()
                    .createWith(recorder.mutinyClient(name, provider, executor.getExecutorProxy()))
                    .destroyer(BeanDestroyer.AutoCloseableDestroyer.class);
            if (single) {
                mutiny.addQualifier(DEFAULT_QUALIFIER);
            }
            syntheticBeans.produce(mutiny.done());
        });
    }

    /** Endpoint name to provider name, in a stable order so the build is reproducible. */
    private static Map<String, String> discoverEndpoints() {
        Config config = ConfigProvider.getConfig();
        Map<String, String> endpoints = new TreeMap<>();
        for (String property : config.getPropertyNames()) {
            if (!property.startsWith(ENDPOINT_PREFIX)) {
                continue;
            }
            String remainder = property.substring(ENDPOINT_PREFIX.length());
            int dot = remainder.indexOf('.');
            if (dot <= 0) {
                continue;
            }
            String name = remainder.substring(0, dot);
            endpoints.computeIfAbsent(name, key -> config
                    .getOptionalValue(ENDPOINT_PREFIX + key + ".provider", String.class)
                    .orElse(DEFAULT_PROVIDER));
        }
        return endpoints;
    }

    private static AnnotationInstance namedSftp(String name) {
        return AnnotationInstance.builder(NamedSftp.class).add("value", name).build();
    }
}
