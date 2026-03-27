package com.amazon.pipeline.domain;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import java.io.Serializable;
import java.util.List;

public class GenericParser implements Serializable {

    public static Row parse(String[] raw, Schema schema, List<FieldMetadata> metadata) {
        Row.Builder builder = Row.withSchema(schema);

        for (FieldMetadata field : metadata) {
            int targetIndex = field.index();
            String value = "";

            if (targetIndex >= 0 && targetIndex < raw.length) {
                value = raw[targetIndex];
            }

            value = cleanCsvNoise(value);

            builder.addValue(value);
        }

        return builder.build();
    }

    private static String cleanCsvNoise(String input) {
        if (input == null) return "";

        String trimmed = input.trim();

        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() > 1) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }

        return trimmed;
    }
}