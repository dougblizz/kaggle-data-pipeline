package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.SaleRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.Metrics;
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
    public void saveRows(PCollection<Row> rows) {
        rows.apply("WriteToMongo", ParDo.of(new DoFn<Row, Void>() {
            // Usamos un cliente estático para evitar fugas de memoria y asegurar persistencia
            private static MongoClient staticClient;
            private transient MongoCollection<Document> collection;
            private final Counter dbWrites = Metrics.counter("MongoAdapter", "db_writes");

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

                    // 1. Limpieza extrema del ID
                    String rawId = row.getString("id");
                    if (rawId == null) {
                        System.err.println("[WARN] ID es nulo, saltando registro.");
                        return;
                    }
                    String idValue = rawId.trim();

                    // 2. Forzamos el _id de Mongo
                    doc.put("_id", idValue);

                    // 3. Ejecutamos y CAPTURAMOS el resultado
                    com.mongodb.client.result.UpdateResult result = collection.replaceOne(
                            Filters.eq("_id", idValue),
                            doc,
                            new ReplaceOptions().upsert(true)
                    );

                    dbWrites.inc();

                    // 4. LOG DE RESULTADO (Esto es la clave)
                    System.out.println(String.format(
                            "[DEBUG-MONGO] ID: %s | Matched: %d | UpsertedId: %s | Modified: %d",
                            idValue,
                            result.getMatchedCount(),
                            result.getUpsertedId(),
                            result.getModifiedCount()
                    ));

                } catch (Exception e) {
                    System.err.println("[CRITICAL-MONGO] Error físico: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }));
    }

    @Override
    public void saveMetrics(MetricQueryResults metrics) {
        // saveMetrics corre en el Driver (tu máquina), no en los workers.
        try (MongoClient client = MongoClients.create(connectionString)) {
            MongoDatabase db = client.getDatabase(databaseName);
            Document report = new Document("timestamp", new java.util.Date());

            metrics.getCounters().forEach(c ->
                    report.append(c.getName().getName(), c.getAttempted()));

            db.getCollection("pipeline_stats").insertOne(report);
            System.out.println(">>> [OK] Métricas guardadas en Mongo.");
        } catch (Exception e) {
            System.err.println(">>> [ERROR] Fallo al guardar métricas: " + e.getMessage());
        }
    }
}