package io.tabular.iceberg.connect.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class CustomTransformation {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Object> advancedFlattenTransformation(Map<String, Object> input) {
        final Map<String, Object> flattenedMap = new LinkedHashMap<>();
        applySchemaless(input, "", flattenedMap);
        return flattenedMap;
    }

    private void applySchemaless(Map<String, Object> originalRecord, String fieldNamePrefix, Map<String, Object> newRecord) {
        for (Map.Entry<String, Object> entry : originalRecord.entrySet()) {
            final String fieldName = fieldName(fieldNamePrefix, entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), null);
                continue;
            }

            Schema.Type inferredType = ConnectSchema.schemaType(value.getClass());
            if (inferredType == null) {
                throw new DataException("Flatten transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }
            switch (inferredType) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case BYTES:
                case ARRAY:
                    newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), value);
                    break;
                case MAP:
                    final Map<String, Object> fieldValue = requireMap(value, "PURPOSE");
                    applySchemaless(fieldValue, fieldName, newRecord);
                    break;
                case STRING:
                    JsonNode jsonNode = validJsonString(String.valueOf(value));
                    if (Objects.nonNull(jsonNode) && jsonNode.size()>0 &&jsonNode.isObject()) {
                        final Map<String, Object> fieldVal = extractMap(jsonNode);
                        applySchemaless(fieldVal, fieldName, newRecord);
                    }else {
                        newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), value);
                    }
                    break;
                default:
                    throw new DataException("Flatten transformation does not support " + value.getClass()
                            + " for record without schemas (for field " + fieldName + ").");
            }
        }
    }

    private String fieldName(String prefix, String fieldName) {
        return prefix.isEmpty() ? fieldName : (prefix + "_" + fieldName);
    }

    private JsonNode validJsonString(String input) {
        try {
            return objectMapper.readTree(input);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private Map<String, Object> extractMap(JsonNode jsonNode) {
        return objectMapper.convertValue(jsonNode, Map.class);
    }
}
