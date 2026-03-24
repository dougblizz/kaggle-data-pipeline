package com.amazon.pipeline.domain;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;


import java.io.Serializable;
import java.util.List;

public class GenericParser implements Serializable {
    public static Row parse(String[] raw, Schema schema, List<FieldMetadata> metadata) {
        Row.Builder builder = Row.withSchema(schema);
        for (FieldMetadata field : metadata) {
            builder.addValue(raw[field.index()]);
        }
        return builder.build();
    }
}
