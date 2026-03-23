package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.AmazonSale;
import com.amazon.pipeline.domain.SaleRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

public class MongoSaleAdapter implements SaleRepository {
    private final String uri;
    private final String dbName;

    public MongoSaleAdapter(String uri, String dbName) {
        this.uri = uri;
        this.dbName = dbName;
    }

    @Override
    public void save(PCollection<AmazonSale> sales) {
        sales.apply("UpsertToMongo", ParDo.of(new MongoUpsertFn(uri, dbName)));
    }

    private static class MongoUpsertFn extends DoFn<AmazonSale, Void> {
        private final String connectionUri;
        private final String databaseName;
        private final MongoSaleMapper mapper;
        private transient MongoClient client;
        private transient MongoCollection<Document> collection;
        private final Counter uniqueCount = Metrics.counter("Sales", "unique_count");

        public MongoUpsertFn(String uri, String dbName) {
            this.connectionUri = uri;
            this.databaseName = dbName;
            this.mapper = new MongoSaleMapper();
        }

        @Setup
        public void setup() {
            client = MongoClients.create(connectionUri);
            collection = client.getDatabase(databaseName).getCollection("sales");
        }

        @ProcessElement
        public void process(@Element AmazonSale s) {
            try {
                Document doc = mapper.toDocument(s);
                collection.replaceOne(Filters.eq("_id", s.id()), doc, new ReplaceOptions().upsert(true));
                uniqueCount.inc();
            } catch (Exception e) {
                System.err.println("Persistence error: " + e.getMessage());
            }
        }

        @Teardown
        public void close() {
            if (client != null) client.close();
        }
    }
}
