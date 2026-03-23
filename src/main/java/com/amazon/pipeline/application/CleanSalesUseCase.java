package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.List;

public class CleanSalesUseCase implements Serializable {
    private final SaleRepository repository;
    private final List<FieldMetadata> metadata;

    public CleanSalesUseCase(SaleRepository repository, List<FieldMetadata> metadata) {
        this.repository = repository;
        this.metadata = metadata;
    }

    public void execute(PCollection<String> rawLines) {
        Schema beamSchema = createSchemaFromMetadata(metadata);

        // Usamos la transformación que encapsula la lógica
        PCollection<Row> cleanRows = rawLines.apply("ProcessData",
                new CleanTransform(metadata, beamSchema));

        repository.saveRows(cleanRows);
    }

    private Schema createSchemaFromMetadata(List<FieldMetadata> metadata) {
        Schema.Builder builder = Schema.builder();
        for (FieldMetadata field : metadata) {
            builder.addStringField(field.name());
        }
        return builder.build();
    }
}