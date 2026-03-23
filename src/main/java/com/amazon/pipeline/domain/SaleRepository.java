package com.amazon.pipeline.domain;

import org.apache.beam.sdk.values.PCollection;

public interface SaleRepository {
    void save(PCollection<AmazonSale> sales);
}
