package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.SaleRepository;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

@RequiredArgsConstructor
public class MongoGenericAdapter implements SaleRepository, Serializable {
    private final String connectionString;
    private final String databaseName;
    private final String collectionName;

    @Override
    public void saveRows(PCollection<Row> rows, String entityKeyField) {
        rows.apply("WriteToMongo", ParDo.of(
                new MongoWriterDoFn(connectionString, databaseName, collectionName, entityKeyField)
        ));
    }
}