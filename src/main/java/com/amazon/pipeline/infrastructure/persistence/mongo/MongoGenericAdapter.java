package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.SaleRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;

import java.io.Serializable;

public class MongoGenericAdapter implements SaleRepository, Serializable {
    private final String connectionString;
    private final String databaseName;

    public MongoGenericAdapter(String connectionString, String databaseName) {
        this.connectionString = connectionString;
        this.databaseName = databaseName;
    }

    @Override
    public void saveRows(PCollection<Row> rows, String entityKeyField) {
        rows.apply("WriteToMongo", ParDo.of(new DoFn<Row, Void>() {
            private static MongoClient staticClient;
            private transient MongoCollection<Document> collection;

            @Setup
            public void setup() {
                if (staticClient == null) {
                    staticClient = MongoClients.create(connectionString);
                }
                this.collection = staticClient.getDatabase(databaseName).getCollection("sales");
            }

            @ProcessElement
            public void process(@Element Row row) {
                try {
                    Document doc = new Document();
                    row.getSchema().getFieldNames().forEach(name ->
                            doc.append(name, row.getValue(name)));

                    // HASHING entityKey
                    String rawId = row.getString(entityKeyField);
                    String hashedId = java.util.Base64.getEncoder().encodeToString(rawId.getBytes());

                    doc.put("_id", hashedId);

                    com.mongodb.client.result.UpdateResult result = collection.replaceOne(
                            Filters.eq("_id", hashedId),
                            doc,
                            new ReplaceOptions().upsert(true)
                    );


                } catch (Exception e) {
                    System.err.println("[CRITICAL-MONGO] Error físico: " + e.getMessage());
                }
            }
        }));
    }
}