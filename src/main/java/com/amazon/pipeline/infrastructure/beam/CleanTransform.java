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
    private final Counter processedCounter = Metrics.counter("CleanTransform", "processed_sales");

    @Override
    public PCollection<AmazonSale> expand(PCollection<String> input) {
        return input
                // PASO 1: Saltar Cabecera
                .apply("SkipHeader", Filter.by(line -> !line.startsWith("index,Order ID")))

                // PASO 2: Limpiar y Parsear usando el Validador de Dominio
                .apply("CleanAndParse", MapElements.into(TypeDescriptor.of(AmazonSale.class))
                        .via(line -> {
                            String[] p = line.split(",");
                            AmazonSale sale = SaleValidator.clean(p);
                            processedCounter.inc();
                            return sale;
                        }));
    }
}