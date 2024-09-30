package io.phasetwo.keycloak.events;

import com.github.xgp.util.BackOff;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.utils.ModelToRepresentation;
import org.keycloak.util.JsonSerialization;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@JBossLog
public class PubSubSenderEventListenerProvider extends SenderEventListenerProvider {

    // Google Cloud PubSub configurations
    protected static final String USER_EVENT_TYPES = "userEventTypes";
    protected static final String ADMIN_EVENT_TYPES = "adminEventTypes";
    protected static final String MESSAGE_ATTRIBUTES = "messageAttributes";
    private Publisher publisher;

    public PubSubSenderEventListenerProvider(KeycloakSession session, ScheduledExecutorService exec) {
        super(session, exec);
    }

    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    @SuppressWarnings("unchecked")
    Set<EventType> getUserEventTypes() {
        Set<EventType> eventTypes = new HashSet<>();

        List<String> events = (List<String>) config.get(USER_EVENT_TYPES);
        if (events != null) {
            events.forEach(event -> eventTypes.add(EventType.valueOf(event.toUpperCase(Locale.US))));
        }

        return eventTypes;
    }

    @SuppressWarnings("unchecked")
    Set<AdminEventType> getAdminEventTypes() {
        Set<AdminEventType> adminEventTypes = new HashSet<>();

        List<String> events = (List<String>) config.get(ADMIN_EVENT_TYPES);
        if (events != null) {
            events.forEach(event -> {
                String[] split = event.split(":");
                adminEventTypes.add(new AdminEventType(OperationType.valueOf(split[0].toUpperCase(Locale.US)),
                        ResourceType.valueOf(split[1].toUpperCase(Locale.US))));
            });
        }

        return adminEventTypes;
    }

    @SuppressWarnings("unchecked")
    Map<String, String> getMessageAttributes() {
        Map<String, String> attributes = new HashMap<>();

        List<String> attribs = (List<String>) config.get(MESSAGE_ATTRIBUTES);
        if (attribs != null) {
            attribs.forEach(attrib -> {
                String[] split = attrib.split(":");
                attributes.put(split[0], split[1]);
            });
        }

        return attributes;
    }

    @Override
    BackOff getBackOff() {
        return BackOff.STOP_BACKOFF;
    }

    @Override
    public void onEvent(Event event) {
        log.infof("onEvent %s %s", event.getType(), event.getId());
        if (!getUserEventTypes().contains(event.getType())) {
            log.debugf("Unregistered user event type: %s.", event.getType());
            return;
        }

        // Process user event
        schedule(new SenderTask(ModelToRepresentation.toRepresentation(event), getBackOff()),
                0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        log.infof("onEvent %s %s %s", event.getOperationType(), event.getResourceType(), event.getResourcePath());
        AdminEventType eventType = new AdminEventType(event.getOperationType(), event.getResourceType());
        if (!getAdminEventTypes().contains(eventType)) {
            log.debugf("Unregistered admin event type: %s.", eventType);
            return;
        }

        // Process admin event
        schedule(new SenderTask(ModelToRepresentation.toRepresentation(event), getBackOff()),
                0L, TimeUnit.MILLISECONDS);
    }

    @Override
    void send(SenderTask task) throws SenderException, IOException {
        final String topic = publisher.getTopicName().getTopic();
        log.infof("Attempting to send %s message to Pub/Sub: Topic: %s, Attributes: %s",
                getUserEventTypes().toString(), topic, getMessageAttributes().toString());

        try {
            String jsonStr = JsonSerialization.writeValueAsString(task.getEvent());
            log.infof("Pub/Sub: Message: %s", jsonStr);

            ByteString data = ByteString.copyFromUtf8(jsonStr);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .putAllAttributes(getMessageAttributes())
                    .setData(data)
                    .build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {

                // Handle message success
                public void onSuccess(String messageId) {
                    log.infof("Message id '%s' sent to Pub/Sub topic '%s' successfully.", messageId, topic);
                }

                // Handle message failure
                public void onFailure(Throwable throwable) {
                    log.errorf(throwable, "Failed to send message to Pub/Sub topic '%s'.", topic);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception exception) {
            log.warnf(exception, "Failed to send message to Pub/Sub topic %s", topic);
            throw new SenderException(false, exception);
        }
    }

    static class AdminEventType {
        public final OperationType operationType;
        public final ResourceType resourceType;

        public AdminEventType(OperationType operationType, ResourceType resourceType) {
            this.operationType = operationType;
            this.resourceType = resourceType;
        }

        @Override
        public String toString() {
            return "AdminEventType {" +
                    "operationType=" + operationType +
                    ", resourceType=" + resourceType +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AdminEventType that = (AdminEventType) o;
            return operationType == that.operationType && resourceType == that.resourceType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(operationType, resourceType);
        }
    }
}
