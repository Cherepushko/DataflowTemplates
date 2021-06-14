package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class AddTimestamp extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new AddTimestampFn()));
    }

    static class AddTimestampFn extends DoFn<String, String> {

        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static final String TIMESTAMP_FIELD_NAME = "@timestamp";

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            String input = context.element();
            JsonNode node = OBJECT_MAPPER.readTree(input);
            if (node.has(TIMESTAMP_FIELD_NAME)) {
                context.output(input);
            } else {
                ObjectNode nodeWithTimestamp = (ObjectNode) node;
                String foundTimestamp = findTimestampValue(node);
                if (foundTimestamp != null) {
                    nodeWithTimestamp.put(TIMESTAMP_FIELD_NAME, foundTimestamp);
                } else {
                    java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
                    Instant instant = timestamp.toInstant();
                    nodeWithTimestamp.put(TIMESTAMP_FIELD_NAME, instant.toString());
                }
                context.output(nodeWithTimestamp.toString());
            }
        }

        private String findTimestampValue(JsonNode node) {
            if (node.has("timestamp")) {
                return node.get("timestamp").asText();
            }

            return null;
        }

    }

}
