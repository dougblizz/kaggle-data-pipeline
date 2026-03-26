package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;

import java.util.Base64;

@Slf4j
@RequiredArgsConstructor
public class MongoWriterDoFn extends DoFn<Row, Void> {
    private static MongoClient staticClient;
    private transient MongoCollection<Document> collection;

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final String keyField;

    @DoFn.Setup
    public void setup() {
        if (staticClient == null) {
            staticClient = MongoClients.create(connectionString);
        }
        this.collection = staticClient.getDatabase(databaseName).getCollection(collectionName);
    }

    @ProcessElement
    public void process(@Element Row row) {
        try {
            Document doc = mapRowToDocument(row);

            String rawKey = row.getString(keyField);

            String hashedId = hashKey(rawKey);

            doc.put("_id", hashedId);

            collection.replaceOne(Filters.eq("_id", hashedId), doc, new ReplaceOptions().upsert(true));
        } catch (Exception e) {
            log.warn("[CRITICAL-MONGO] Failed to persist record. Key: {}, Error: {}", keyField, e.getMessage());
        }
    }

    private Document mapRowToDocument(Row row) {
        Document doc = new Document();
        row.getSchema().getFieldNames().forEach(n -> doc.append(n, row.getValue(n)));
        return doc;
    }

    private String hashKey(String rawKey) {
        return Base64.getEncoder().encodeToString(rawKey.getBytes());
    }
}
