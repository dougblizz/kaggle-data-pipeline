package com.amazon.pipeline;

import com.amazon.pipeline.application.CleanSalesUseCase;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.persistence.MongoSaleAdapter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class MainPipeline {
    public static void main(String[] args) {
        AmazonPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(AmazonPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> input = p.apply("ReadCSV",
                TextIO.read().from(options.getInputFile()));

        SaleRepository repository = new MongoSaleAdapter();
        CleanSalesUseCase useCase = new CleanSalesUseCase(repository);

        // company logic
        useCase.execute(input);

        // get result
        PipelineResult result = p.run();
        result.waitUntilFinish();


        // get metrics
        MetricResults metrics = result.metrics();
        MetricQueryResults results = metrics.queryMetrics(MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named("CleanTransform", "processed_sales"))
                .addNameFilter(MetricNameFilter.named("Sales", "unique_count"))
                .build());

        // print metrics - para local mientras
        for (MetricResult<Long> counter : results.getCounters()) {
            System.out.println(">>> METRICA: " + counter.getName().getName() +
                    " = " + counter.getAttempted());
        }
    }
}