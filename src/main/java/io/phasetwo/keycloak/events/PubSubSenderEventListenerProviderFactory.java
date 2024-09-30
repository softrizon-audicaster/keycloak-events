package io.phasetwo.keycloak.events;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.TopicName;
import io.phasetwo.keycloak.config.PubSubConfig;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@JBossLog
@AutoService(EventListenerProviderFactory.class)
public class PubSubSenderEventListenerProviderFactory extends MultiEventListenerProviderFactory {

    public static final String PROVIDER_ID = "ext-event-pubsub";
    public static final String CLASS_NAME = PubSubSenderEventListenerProviderFactory.class.getCanonicalName();

    private GoogleCredentials credentials;
    private Publisher publisher;
    private PubSubConfig config;
    private ScheduledExecutorService exec;

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public MultiEventListenerProvider create(KeycloakSession session) {
        if (publisher == null) {
            try {
                publisher = Publisher.newBuilder(TopicName.of(config.getProjectId(), config.getTopicId()))
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .build();
            } catch (IOException exception) {
                log.error("Failed to create Pub/Sub publisher", exception);
            }

            Objects.requireNonNull(publisher, String.format("%s: Google Pub/Sub publisher is required.", CLASS_NAME));
        }

        return super.create(session);
    }

    @Override
    protected EventListenerProvider configure(KeycloakSession session, Map<String, Object> config) {
        PubSubSenderEventListenerProvider provider = new PubSubSenderEventListenerProvider(session, exec);
        log.infof("Configuring %s with %s", provider.getClass().getName(), configToString(config));
        provider.setConfig(config);
        provider.setPublisher(publisher);
        return provider;
    }

    @Override
    public void init(Config.Scope scope) {
        config = PubSubConfig.create(scope);

        try {
            credentials = GoogleCredentials.fromStream(Files.newInputStream(Paths.get(config.getSaCredentialsPath())));
        } catch (IOException exception) {
            log.error("Failed to load Google service account.", exception);
        }

        Objects.requireNonNull(credentials, String.format("%s: Google credentials is required.", CLASS_NAME));

        int processors = Runtime.getRuntime().availableProcessors();
        exec = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(processors));
    }

    @Override
    public void close() {
        try {
            log.info("Shutting down scheduler");
            exec.shutdown();
        } catch (Exception e) {
            log.warn("Error in shutdown of scheduler", e);
        }
    }

    @Override
    protected boolean isAsync() {
        return true;
    }
}
