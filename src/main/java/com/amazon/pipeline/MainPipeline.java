package com.amazon.pipeline;

import com.amazon.pipeline.application.CleanSalesUseCase;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.persistence.PostgresSaleAdapter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class MainPipeline {
    public static void main(String[] args) {
        AmazonPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(AmazonPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> input = p.apply("ReadCSV",
                TextIO.read().from(options.getInputFile()));

        //db - broken right now
        SaleRepository repository = new PostgresSaleAdapter();
        CleanSalesUseCase useCase = new CleanSalesUseCase(repository);

        // company logic
        useCase.execute(input);

        // get result
        PipelineResult result = p.run();
        result.waitUntilFinish();


        // get metrics
        MetricResults metrics = result.metrics();
        MetricQueryResults results = metrics.queryMetrics(MetricsFilter.builder() // <--- MetricsFilter
                .addNameFilter(MetricNameFilter.named("Sales", "processed_count"))
                .build());

        // print metrics
        for (MetricResult<Long> counter : results.getCounters()) {
            System.out.println(">>> REGISTROS PROCESADOS: " + counter.getAttempted());
        }
    }
}