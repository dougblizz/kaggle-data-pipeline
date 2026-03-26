package com.amazon.pipeline.infrastructure;

import com.amazon.pipeline.application.CleanSalesUseCase;
import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.persistence.mongo.MetadataLoader;
import com.amazon.pipeline.infrastructure.persistence.mongo.MongoGenericAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

@Slf4j
public class MainPipeline {
    public static void main(String[] args) {
        AmazonPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(AmazonPipelineOptions.class);

        System.setProperty("beamDirectRunnerMaxBundleSize", "10000");
        System.setProperty("targetParallelism", "1");

        Pipeline p = Pipeline.create(options);

        PCollection<String> input = p.apply("ReadCSV",
                TextIO.read().from(options.getInputFile()));

        // carga metadata  - path
        List<FieldMetadata> metadata = MetadataLoader.load("src/main/resources/pipeline-metadata.json");

        // connection db
        SaleRepository repository = new MongoGenericAdapter(
                options.getMongoUri(),
                options.getDatabase(),
                options.getCollection()
        );

        CleanSalesUseCase useCase = new CleanSalesUseCase(repository, metadata);

        // company logic
        useCase.execute(input);

        // get result
        PipelineResult result = p.run();
        result.waitUntilFinish();

        log.info(">>> Pipeline complete. Forcing out.");
        System.exit(0);

        MetricsFilter filter = MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named("CleanTransform", "processed_sales"))
                .addNameFilter(MetricNameFilter.named("Sales", "unique_count"))
                .build();

        MetricQueryResults results = result.metrics().queryMetrics(filter);

        // print metrics - para local mientras
        for (MetricResult<Long> counter : results.getCounters()) {
            log.info(">>> METRIC: {} = {}", counter.getName().getName(), counter.getAttempted());
        }
    }
}