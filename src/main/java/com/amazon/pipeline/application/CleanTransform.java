package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.*;
import com.amazon.pipeline.infrastructure.utils.AppConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nonnull;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class CleanTransform extends PTransform<PCollection<String>, PCollection<Row>> {

    private final List<FieldMetadata> metadata;
    private final Schema schema;

    @Override
    @Nonnull
    public PCollection<Row> expand(PCollection<String> input) {
        return input.apply("ParseAndCleanRows",
                        ParDo.of(new CleanDataFn(metadata, schema)))
                .setRowSchema(schema);
    }

    /**
     * Internal static class: Does not capture the CleanTransform state.
     * It is serializable by nature and easy to move between workers.
     */
    @RequiredArgsConstructor
    private static class CleanDataFn extends DoFn<String, Row> {

        private final List<FieldMetadata> metadata;
        private final Schema schema;
        private final Counter processedCounter = Metrics.counter("CleanTransform", "processed_sales");

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Row> out) {
            if (line == null || line.isEmpty() || line.startsWith("id,")) return;

            try {
                processedCounter.inc();

                String[] fields = AppConstants.CSV_PATTERN.split(line, -1);

                Row rawRow = GenericParser.parse(fields, schema, metadata);

                Row.Builder rowBuilder = Row.withSchema(schema);
                for (FieldMetadata field : metadata) {
                    String rawValue = rawRow.getString(field.name());
                    rowBuilder.addValue(applyCleaningLogic(rawValue, field));
                }

                out.output(rowBuilder.build());

            } catch (Exception e) {
                // In production, this is where you would send a "Dead Letter Queue"
                log.error("Error parsing line: {}", line);
            }
        }

        private String applyCleaningLogic(String value, FieldMetadata field) {
            if (value == null) return "";
            if ("amount".equalsIgnoreCase(field.name())) {
                return DataCleanser.formatAmount(value);
            }
            return DataCleanser.normalizeText(value);
        }
    }
}