package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.AmazonSale;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.beam.CleanTransform;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CleanSalesUseCase {
    private final SaleRepository repository;

    public CleanSalesUseCase(SaleRepository repository) {
        this.repository = repository;
    }

    public void execute(PCollection<String> rawLines) {
        PCollection<AmazonSale> domainSales = rawLines.apply("CleanData", new CleanTransform());

        // Deduplication by ID before persisting
        PCollection<AmazonSale> uniqueSales = domainSales.apply("DeduplicateById",
                Distinct.withRepresentativeValueFn(AmazonSale::id)
                        .withRepresentativeType(TypeDescriptors.strings()));

        repository.save(uniqueSales);
    }
}