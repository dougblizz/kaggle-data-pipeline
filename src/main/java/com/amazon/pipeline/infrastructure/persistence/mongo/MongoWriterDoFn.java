package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.utils.HashUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazon.pipeline.domain.utils.HashUtils.generateId;
import static com.amazon.pipeline.infrastructure.persistence.mongo.mappers.MongoDocumentMapper.toDocument;

@Slf4j
@RequiredArgsConstructor
public class MongoWriterDoFn extends DoFn<Row, Void> {
    private final String connectionString;
    private final String dbName;
    private final String collectionName;
    private final String keyField;
    private final int batchSize;

    // Buffer local por cada hilo (Worker)
    private transient List<WriteModel<Document>> bulkOperations;
    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;

    @Setup
    public void setup() {
        this.mongoClient = MongoClients.create(connectionString);
        this.collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        this.bulkOperations = new ArrayList<>();
    }

    @ProcessElement
    public void process(@Element Row row) {
        try {
            String rawId = row.getString(keyField);
            if (rawId == null) return;

            // 1. Normalización y Hashing (Consistencia total)
            String normalizedId = HashUtils.normalize(rawId);
            String hashedId = HashUtils.generateId(normalizedId);

            Document doc = toDocument(row);
            doc.put("_id", hashedId); // ID único para evitar duplicados en Mongo

            // 2. Agregamos a la lista de operaciones Bulk (Upsert)
            bulkOperations.add(new ReplaceOneModel<>(
                    Filters.eq("_id", hashedId),
                    doc,
                    new ReplaceOptions().upsert(true)
            ));

            // 3. Si llegamos al límite, disparamos el Bulk
            if (bulkOperations.size() >= batchSize) {
                flush();
            }
        } catch (Exception e) {
            log.error("[MONGO-PROCESS-ERROR] Error preparando registro: {}", e.getMessage());
        }
    }

    @FinishBundle
    public void finishBundle() {
        flush(); // El salvavidas de los últimos registros
    }

    private void flush() {
        if (bulkOperations.isEmpty()) return;

        try {
            // Ejecución masiva: Mucho más rápido que insertar uno por uno
            collection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(false));
            bulkOperations.clear();
        } catch (Exception e) {
            log.error("[MONGO-FLUSH-ERROR] Error en escritura masiva: {}", e.getMessage());
            // Aquí podrías implementar una lógica de reintento o Dead Letter Queue
        }
    }

    @Teardown
    public void teardown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
