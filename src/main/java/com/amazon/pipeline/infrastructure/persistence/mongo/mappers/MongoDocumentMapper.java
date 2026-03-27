package com.amazon.pipeline.infrastructure.persistence.mongo.mappers;

import org.apache.beam.sdk.values.Row;
import org.bson.Document;

public class MongoDocumentMapper {
    public static Document toDocument(Row row) {
        Document doc = new Document();
        row.getSchema().getFieldNames().forEach(n -> {
            Object value = row.getValue(n);
            if (value != null) doc.append(n, value);
        });
        return doc;
    }
}
