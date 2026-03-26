package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class CleanSalesUseCase implements Serializable {
    private final SaleRepository repository;
    private final List<FieldMetadata> metadata;

    public void execute(PCollection<String> rawLines) {
        try {
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

            // Filter out invalid rows before deduplication
            PCollection<Row> validRows = cleanRows.apply("FilterInvalidRows",
                    Filter.by(row -> isValidRow(row, entityKeyField)));

            // Deduplication and metric for unique records
            PCollection<Row> distinctRows = validRows.apply("DeduplicateByEntityKey",
                    Distinct.withRepresentativeValueFn(row -> row.getString(entityKeyField)));

            // Metrics
            distinctRows.apply("CountUnique", Count.globally())
                    .apply("LogMetrics", ParDo.of(new DoFn<Long, Void>() {
                        @ProcessElement
                        public void process(@Element Long count) {
                            log.info("Total unique records to persist: {}", count);
                        }
                    }));

            repository.saveRows(distinctRows, entityKeyField);
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