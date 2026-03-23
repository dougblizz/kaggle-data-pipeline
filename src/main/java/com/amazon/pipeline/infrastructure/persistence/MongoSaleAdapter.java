package com.amazon.pipeline.infrastructure.persistence;

import com.amazon.pipeline.domain.AmazonSale;
import com.amazon.pipeline.domain.SaleRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.bson.Document;

public class MongoSaleAdapter implements SaleRepository {

    @Override
    public void save(PCollection<AmazonSale> sales) {
        sales
                // 1. DEDUPLICACIÓN EN BEAM: Filtra duplicados por ID antes de seguir
                .apply("DeduplicateById", Distinct.withRepresentativeValueFn(AmazonSale::id)
                        .withRepresentativeType(TypeDescriptors.strings()))
                // 2. ESCRITURA: Ahora solo llegan los ~120,378 registros únicos
                .apply("UpsertToMongo", ParDo.of(new MongoUpsertFn()));
    }

    private static class MongoUpsertFn extends DoFn<AmazonSale, Void> {
        private final Counter uniqueCount = Metrics.counter("Sales", "unique_count");
        private transient MongoClient client;
        private transient MongoCollection<Document> collection;

        @Setup
        public void setup() {
            String uri = "mongodb://admin:secret_pass@localhost:27017";
            client = MongoClients.create(uri);
            collection = client.getDatabase("amazon_data").getCollection("sales");
        }

        @ProcessElement
        public void process(@Element AmazonSale s) {
            try {
                Document doc = new Document("_id", s.id())
                        .append("amt", s.amount()).append("cat", s.category());

                collection.replaceOne(Filters.eq("_id", s.id()), doc, new ReplaceOptions().upsert(true));
                uniqueCount.inc();
            } catch (Exception e) {
                System.err.println("Error procesando ID " + s.id() + ": " + e.getMessage());
            }
        }

        @Teardown
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
