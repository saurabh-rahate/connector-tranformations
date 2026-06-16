package com.haha.transformers;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CustomKeyTransformTest {

    private CustomKeyTransform<SinkRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new CustomKeyTransform<>();
        transform.configure(java.util.Collections.emptyMap());
    }

    private SinkRecord buildRecord(Object key) {
        return new SinkRecord(
                "test-topic",
                0,
                null,                  // keySchema
                key,
                null,                  // valueSchema
                "{\"name\":\"test\"}",  // value (not used in key logic)
                0L
        );
    }

    @Test
    void shouldRemoveIdWhenOtherFieldsExist() {
        String key = "{\"_id\": \"6a304822680d2d8d40b6d2f2\", \"anyOtherKey\": \"otherKey\"}";
        SinkRecord record = buildRecord(key);

        SinkRecord result = transform.apply(record);

        assertEquals("{\"anyOtherKey\":\"otherKey\"}", result.key());
    }

    @Test
    void shouldKeepIdWhenItIsTheOnlyField() {
        String key = "{\"_id\": \"6a304822680d2d8d40b6d2f2\"}";
        SinkRecord record = buildRecord(key);

        SinkRecord result = transform.apply(record);

        assertEquals("{\"_id\":\"6a304822680d2d8d40b6d2f2\"}", result.key());
    }

    @Test
    void shouldKeepKeyUnchangedWhenNoIdField() {
        String key = "{\"anyOtherKey\": \"otherKey\"}";
        SinkRecord record = buildRecord(key);

        SinkRecord result = transform.apply(record);

        assertEquals("{\"anyOtherKey\":\"otherKey\"}", result.key());
    }

    @Test
    void shouldReturnRecordAsIsWhenKeyIsNull() {
        SinkRecord record = buildRecord(null);

        SinkRecord result = transform.apply(record);

        assertNull(result.key());
    }

    @Test
    void shouldReturnRecordAsIsWhenKeyIsMalformedJson() {
        String malformedKey = "{not-valid-json";
        SinkRecord record = buildRecord(malformedKey);

        SinkRecord result = transform.apply(record);

        // Exception caught internally -> original record returned unchanged
        assertEquals(malformedKey, result.key());
    }

    @Test
    void shouldKeepEmptyObjectKeyUnchanged() {
        String key = "{}";
        SinkRecord record = buildRecord(key);

        SinkRecord result = transform.apply(record);

        assertEquals("{}", result.key());
    }

    @Test
    void shouldRemoveIdWhenIdIsNestedObject() {
        String key = "{\"_id\": {\"$oid\": \"6a304822680d2d8d40b6d2f2\"}, \"anyOtherKey\": \"otherKey\"}";
        SinkRecord record = buildRecord(key);

        SinkRecord result = transform.apply(record);

        assertEquals("{\"anyOtherKey\":\"otherKey\"}", result.key());
    }

    @Test
    void shouldPreserveTopicPartitionAndValueOnTransformation() {
        String key = "{\"_id\": \"123\", \"anyOtherKey\": \"otherKey\"}";
        SinkRecord record = buildRecord(key);

        SinkRecord result = transform.apply(record);

        assertEquals(record.topic(), result.topic());
        assertEquals(record.kafkaPartition(), result.kafkaPartition());
        assertEquals(record.value(), result.value());
    }
}