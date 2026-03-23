package com.amazon.pipeline.infrastructure.beam;

import com.amazon.pipeline.domain.AmazonSale;
import com.amazon.pipeline.domain.SaleValidator;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;


public class CleanTransform extends PTransform<PCollection<String>, PCollection<AmazonSale>> {

    // Metric for every file
    private final Counter processedSales = Metrics.counter("Sales", "processed_count");

    @Override
    public PCollection<AmazonSale> expand(PCollection<String> input) {
        PCollection<String> dataOnly = input.apply("SkipHeader",
                Filter.by((String line) -> !line.startsWith("index,Order ID")));

        return dataOnly.apply("LogAndClean", MapElements
                .into(TypeDescriptor.of(AmazonSale.class))
                .via((String line) -> {
                    processedSales.inc();
                    return SaleValidator.clean(line.split(","));
                }))
                .setCoder(SerializableCoder.of(AmazonSale.class));
    }
}