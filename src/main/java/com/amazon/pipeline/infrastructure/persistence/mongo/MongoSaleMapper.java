package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.AmazonSale;
import org.bson.Document;

import java.io.Serializable;

class MongoSaleMapper implements Serializable {
    public Document toDocument(AmazonSale sale) {
        return new Document("_id", sale.id())
                .append("amt", sale.amount())
                .append("cat", sale.category());
    }
}