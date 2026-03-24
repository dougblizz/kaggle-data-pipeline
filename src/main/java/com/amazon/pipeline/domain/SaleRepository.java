package com.amazon.pipeline.domain;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface SaleRepository {
    void saveRows(PCollection<Row> rows, String entityKeyField);
}
