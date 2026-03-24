package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import java.util.List;

public class CleanTransform extends PTransform<PCollection<String>, PCollection<Row>> {
    private final List<FieldMetadata> metadata;
    private final Schema schema;
    private final Counter processedCounter = Metrics.counter("CleanTransform", "processed_sales");

    public CleanTransform(List<FieldMetadata> metadata, Schema schema) {
        this.metadata = metadata;
        this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> input) {
        return input
                .apply("ParseAndClean", MapElements.into(TypeDescriptors.rows())
                        .via(line -> {
                            processedCounter.inc();
                            String[] fields = line.split(",");

                            // dynamic parsing
                            Row rawRow = GenericParser.parse(fields, schema, metadata);

                            // Dynamic cleaning (using the JSON names)
                            return Row.withSchema(schema)
                                    .addValue(rawRow.getString("id"))
                                    .addValue(DataCleanser.formatAmount(rawRow.getString("amount")))
                                    .addValue(DataCleanser.normalizeText(rawRow.getString("category")))
                                    .build();
                        }))
                .setRowSchema(schema);
    }
}