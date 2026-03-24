package com.amazon.pipeline;

import com.amazon.pipeline.application.CleanSalesUseCase;
import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.MetadataLoader;
import com.amazon.pipeline.infrastructure.persistence.mongo.MongoGenericAdapter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class MainPipeline {
    public static void main(String[] args) {
        AmazonPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(AmazonPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> input = p.apply("ReadCSV",
                TextIO.read().from(options.getInputFile()));

        // carga metadata  - path
        List<FieldMetadata> metadata = MetadataLoader.load("src/main/resources/pipeline-metadata.json");

        // connection db
        SaleRepository repository = new MongoGenericAdapter(
                "mongodb://admin:secret_pass@localhost:27017",
                "amazon_data"
        );

        CleanSalesUseCase useCase = new CleanSalesUseCase(repository, metadata);

        // company logic
        useCase.execute(input);

        // get result
        PipelineResult result = p.run();
        result.waitUntilFinish();

        MetricsFilter filter = MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named("CleanTransform", "processed_sales"))
                .addNameFilter(MetricNameFilter.named("Sales", "unique_count"))
                .build();

        MetricQueryResults results = result.metrics().queryMetrics(filter);

        // print metrics - para local mientras
        for (MetricResult<Long> counter : results.getCounters()) {
            System.out.println(">>> METRIC: " + counter.getName().getName() +
                    " = " + counter.getAttempted());
        }
    }
}