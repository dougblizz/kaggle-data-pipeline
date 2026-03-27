package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazon.pipeline.domain.utils.HashUtils.sha256Hash;
import static com.amazon.pipeline.infrastructure.persistence.mongo.mappers.MongoDocumentMapper.toDocument;

@Slf4j
@RequiredArgsConstructor
public class MongoWriterDoFn extends DoFn<Row, Void> {
    private static final int BATCH_SIZE = 500;
    private static final int MAX_RETRIES = 3;
    private static volatile MongoClient staticClient;
    private static final List<WriteModel<Document>> SHARED_BATCH = Collections.synchronizedList(new ArrayList<>());
    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final String keyField;

    @Setup
    public void setup() {
        if (staticClient == null) {
            synchronized (MongoWriterDoFn.class) {
                if (staticClient == null) {
                    staticClient = MongoClients.create(connectionString);
                }
            }
        }
    }

    @ProcessElement
    public void process(@Element Row row) {
        try {
            Document doc = toDocument(row);
            String rawId = row.getString(keyField);

            if (rawId == null) return;

            String hashedId = sha256Hash(rawId.trim());
            doc.put("_id", hashedId);

            SHARED_BATCH.add(new ReplaceOneModel<>(
                    Filters.eq("_id", hashedId),
                    doc,
                    new ReplaceOptions().upsert(true)
            ));

            if (SHARED_BATCH.size() >= BATCH_SIZE) {
                flush();
            }
        } catch (Exception e) {
            log.error("[MONGO-PROCESS-ERROR] {}", e.getMessage());
        }
    }

    @FinishBundle
    public void finishBundle() {
        if (SHARED_BATCH.size() >= BATCH_SIZE) {
            flush();
        }
    }

    @Teardown
    public void teardown() {
        synchronized (MongoWriterDoFn.class) {
            if (staticClient != null) {
                log.info("[MONGO-SHUTDOWN] Closing connection pool...");
                staticClient.close();
                staticClient = null;
            }
        }
    }

    private void flush() {
        if (SHARED_BATCH.isEmpty()) return;

        synchronized (SHARED_BATCH) {
            if (SHARED_BATCH.isEmpty()) return;

            MongoCollection<Document> collection = staticClient
                    .getDatabase(databaseName)
                    .getCollection(collectionName);

            List<WriteModel<Document>> toWrite = new ArrayList<>(SHARED_BATCH);
            SHARED_BATCH.clear();

            int attempts = 0;
            while (attempts < MAX_RETRIES) {
                try {
                    log.warn("[MONGO-BULK] Writing batch of {} records...", toWrite.size());
                    collection.bulkWrite(toWrite);
                    log.warn("[MONGO-SUCCESS] Writing completed.");
                    return;
                } catch (Exception e) {
                    attempts++;
                    log.warn("[MONGO-RETRY] Attempt {} failed: {}", attempts, e.getMessage());
                    if (attempts >= MAX_RETRIES) {
                        log.error("[CRITICAL] {} records were lost", toWrite.size());
                    } else {
                        try { Thread.sleep(1000L * attempts); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    }
                }
            }
        }
    }
}
