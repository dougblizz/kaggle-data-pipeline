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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class MongoWriterDoFn extends DoFn<Row, Void> {
    private static final int BATCH_SIZE = 500;
    private static final int MAX_RETRIES = 3;

    // volatile asegura que todos los hilos vean el mismo estado del cliente
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
            Document doc = mapRowToDocument(row);
            String rawId = row.getString(keyField);

            // Validación de seguridad para la Key
            if (rawId == null) return;

            String hashedId = sha256Hash(rawId);
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
                log.info("[MONGO-SHUTDOWN] Cerrando pool de conexiones...");
                staticClient.close();
                staticClient = null;
            }
        }
    }

    private void flush() {
        if (SHARED_BATCH.isEmpty()) return;

        synchronized (SHARED_BATCH) {
            if (SHARED_BATCH.isEmpty()) return;

            // OBTENER LA COLECCIÓN AQUÍ (Evita el NullPointerException)
            MongoCollection<Document> collection = staticClient
                    .getDatabase(databaseName)
                    .getCollection(collectionName);

            List<WriteModel<Document>> toWrite = new ArrayList<>(SHARED_BATCH);
            SHARED_BATCH.clear();

            int attempts = 0;
            while (attempts < MAX_RETRIES) {
                try {
                    log.warn("[MONGO-BULK] Escribiendo lote de {} registros...", toWrite.size());
                    collection.bulkWrite(toWrite);
                    log.warn("[MONGO-SUCCESS] Escritura completada.");
                    return;
                } catch (Exception e) {
                    attempts++;
                    log.warn("[MONGO-RETRY] Intento {} fallido: {}", attempts, e.getMessage());
                    if (attempts >= MAX_RETRIES) {
                        log.error("[CRITICAL] Se perdieron {} registros", toWrite.size());
                    } else {
                        try { Thread.sleep(1000L * attempts); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    }
                }
            }
        }
    }

    private Document mapRowToDocument(Row row) {
        Document doc = new Document();
        row.getSchema().getFieldNames().forEach(n -> {
            Object value = row.getValue(n);
            if (value != null) doc.append(n, value);
        });
        return doc;
    }

    private String sha256Hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedHash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(64);
            for (byte b : encodedHash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            return Base64.getEncoder().encodeToString(input.getBytes()); // Fallback
        }
    }
}
