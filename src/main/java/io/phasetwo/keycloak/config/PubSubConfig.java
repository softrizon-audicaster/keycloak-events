package io.phasetwo.keycloak.config;

import lombok.extern.jbosslog.JBossLog;
import org.keycloak.Config;

import java.util.Locale;
import java.util.Objects;

@JBossLog
public class PubSubConfig {

    public static final String CLASS_NAME = PubSubConfig.class.getCanonicalName();

    private String saCredentialsPath;
    private String projectId;
    private String topicId;

    public String getSaCredentialsPath() {
        return saCredentialsPath;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getTopicId() {
        return topicId;
    }

    public static PubSubConfig create(Config.Scope scope) {
        PubSubConfig config = new PubSubConfig();

        // Process the service account google credentials
        config.saCredentialsPath = resolveConfigVariable(scope, "gc_sa_credentials_path", null);
        Objects.requireNonNull(config.saCredentialsPath,
                String.format("%s: the Google service account path is required.", CLASS_NAME));

        // Process the project id
        config.projectId = resolveConfigVariable(scope, "gc_project_id", null);
        Objects.requireNonNull(config.projectId, String.format("%s: the Google project id is required.", CLASS_NAME));

        // Process the topic id
        config.topicId = resolveConfigVariable(scope, "gc_pubsub_topic_id", null);
        Objects.requireNonNull(config.topicId,
                String.format("%s: the Google Pub/Sub topic id is required.", CLASS_NAME));

        return config;
    }

    private static String resolveConfigVariable(Config.Scope scope, String variable, String defaultValue) {
        Objects.requireNonNull(variable, String.format("%s: a variable name is required.", CLASS_NAME));

        String value = defaultValue;
        String newVariable = variable;

        // Extract the value for this variable
        if (scope != null && scope.get(variable) != null) {
            value = scope.get(variable);
        } else { // Try to retrieve the value for this variable from environment variables. Eg: GC_PUBSUB_TOPIC_ID.
            newVariable = variable.toUpperCase(Locale.US);
            String tmpValue = System.getenv(newVariable);
            if (tmpValue != null) {
                value = tmpValue;
            }
        }

        log.infof("Configuration: %s=%s", newVariable, value);

        return value;
    }
}
