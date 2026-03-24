package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

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

        // Search entityKey, if it fails then throw a custom Exception
        String entityKeyField = metadata.stream()
                .filter(FieldMetadata::isEntityKey)
                .map(FieldMetadata::name)
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        "BUSINESS ERROR: No 'isEntityKey' has been defined in the metadata. It is mandatory to designate a field as a unique key."
                ));

        // Transform and cleansing
        PCollection<Row> cleanRows = rawLines.apply("ProcessData",
                new CleanTransform(metadata, beamSchema));

        // Deduplication and metric for unique records
        PCollection<Row> distinctRows = cleanRows
                .apply("DeduplicateByEntityKey", Distinct.<Row, String>withRepresentativeValueFn(row -> row.getString(entityKeyField))
                        .withRepresentativeType(TypeDescriptors.strings()))
                .apply("CountUnique", ParDo.of(new DoFn<Row, Row>() {
                    private final Counter uniqueCounter = Metrics.counter("Sales", "unique_count");
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> out) {
                        uniqueCounter.inc();
                        out.output(row);
                    }
                }))
                .setRowSchema(beamSchema);

        repository.saveRows(distinctRows, entityKeyField);
    }

    private Schema createSchemaFromMetadata(List<FieldMetadata> metadata) {
        Schema.Builder builder = Schema.builder();
        for (FieldMetadata field : metadata) {
            builder.addStringField(field.name());
        }
        return builder.build();
    }
}