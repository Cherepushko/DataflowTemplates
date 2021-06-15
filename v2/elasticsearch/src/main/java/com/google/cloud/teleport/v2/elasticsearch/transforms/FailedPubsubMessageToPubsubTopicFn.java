package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * The {@link FailedPubsubMessageToPubsubTopicFn} converts PubSub message which have failed processing into
 * {@link com.google.api.services.pubsub.model.PubsubMessage} objects which can be output to a PubSub topic.
 */
public class FailedPubsubMessageToPubsubTopicFn
        extends DoFn<FailsafeElement<PubsubMessage, String>, PubsubMessage> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ProcessElement
    public void processElement(ProcessContext context) {
        FailsafeElement<PubsubMessage, String> failsafeElement = context.element();
        PubsubMessage pubsubMessage = failsafeElement.getOriginalPayload();
        String message =
                pubsubMessage.getPayload().length > 0
                        ? new String(pubsubMessage.getPayload())
                        : pubsubMessage.getAttributeMap().toString();

        // Format the timestamp for insertion
        java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
        Instant instant = timestamp.toInstant();

        // Build the output PubSub message
        ObjectNode outputMessage = OBJECT_MAPPER.createObjectNode();
        outputMessage
                .put("@timestamp", instant.toString())
                .put("errorMessage", failsafeElement.getErrorMessage())
                .put("stacktrace", failsafeElement.getStacktrace())
                .put("payloadString", message);

        context.output(new PubsubMessage(outputMessage.toString().getBytes(StandardCharsets.UTF_8), null));
    }

}
