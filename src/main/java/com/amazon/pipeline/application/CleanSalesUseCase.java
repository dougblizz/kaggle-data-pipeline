package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.domain.utils.HashUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

@Slf4j
public class CleanSalesUseCase implements Serializable {
    private final SaleRepository repository;
    private final List<FieldMetadata> metadata;
    private final String entityKeyField;

    public CleanSalesUseCase(SaleRepository repository, List<FieldMetadata> metadata) {
        this.repository = repository;
        this.metadata = metadata;

        // Calculamos la llave una sola vez al instanciar el caso de uso
        this.entityKeyField = metadata.stream()
                .filter(FieldMetadata::isEntityKey)
                .map(FieldMetadata::name)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "BUSINESS ERROR: No 'isEntityKey' has been defined in the metadata."
                ));
    }

    public void execute(PCollection<String> rawLines) {
        try {
            Schema beamSchema = createSchemaFromMetadata(metadata);

            // Transform and cleansing
            PCollection<Row> cleanRows = rawLines.apply("ProcessData",
                            new CleanTransform(metadata, beamSchema))
                    .setRowSchema(beamSchema);

            // Filter out invalid rows before deduplication
            PCollection<Row> validRows = cleanRows.apply("FilterInvalidRows",
                    Filter.by(row -> isValidRow(row, entityKeyField)));

            // Deduplication and metric for unique records
            PCollection<Row> distinctRows = validRows
                    .apply("DistinctByNormalizedId", Distinct.<Row, String>withRepresentativeValueFn(row ->
                                    HashUtils.normalize(row.getString(entityKeyField))
                            )
                            .withRepresentativeType(TypeDescriptor.of(String.class)));


            // Metrics
            PCollection<Row> finalRows = distinctRows.apply("MetricsAndLog", ParDo.of(new DoFn<Row, Row>() {
                        private final Counter uniqueCounter = Metrics.counter("Sales", "unique_count");
                        @ProcessElement
                        public void processElement(@Element Row row, OutputReceiver<Row> out) {
                            uniqueCounter.inc();
                            out.output(row);
                        }
                    }))
                    .setRowSchema(beamSchema);

            repository.saveRows(finalRows, entityKeyField);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error cleansing records in pipeline. Refer to CleanTransform method for validation logic.");
        }
    }

    private static boolean isValidRow(Row row, String entityKeyField) {
        if (row == null) {
            log.warn("Discarding null row");
            return false;
        }
        String key = row.getString(entityKeyField);
        if (StringUtils.isBlank(key)) {
            log.warn("Discarding row with null or empty entity key: {}", entityKeyField);
            return false;
        }
        return true;
    }

    private Schema createSchemaFromMetadata(List<FieldMetadata> metadata) {
        Schema.Builder builder = Schema.builder();
        for (FieldMetadata field : metadata) {
            builder.addStringField(field.name());
        }
        return builder.build();
    }
}