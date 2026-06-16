package com.haha.transformers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class CustomKeyTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public R apply(R record) {
        try {
            Object key = record.key();

            if (key == null) {
                return record;
            }

            String keyStr = key.toString();
            JsonNode keyNode = MAPPER.readTree(keyStr);

            if (!(keyNode instanceof ObjectNode)) {
                return record;
            }

            ObjectNode objectNode = (ObjectNode) keyNode;

            // If key has _id AND other fields, remove _id
            if (objectNode.has("_id") && objectNode.size() > 1) {
                objectNode.remove("_id");
            }

            String newKey = MAPPER.writeValueAsString(objectNode);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    newKey,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );

        } catch (Exception e) {
            return record;
        }
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Cleanup resources if any
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Read config values here if you defined any in CONFIG_DEF
    }
}