package com.amazon.pipeline.application;

import com.amazon.pipeline.domain.AmazonSale;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.beam.CleanTransform;
import org.apache.beam.sdk.values.PCollection;

public class CleanSalesUseCase {
    private final SaleRepository repository;

    public CleanSalesUseCase(SaleRepository repository) {
        this.repository = repository;
    }

    public void execute(PCollection<String> rawLines) {
        // Transform raw values
        PCollection<AmazonSale> domainSales = rawLines.apply(new CleanTransform());

        repository.save(domainSales);
    }
}