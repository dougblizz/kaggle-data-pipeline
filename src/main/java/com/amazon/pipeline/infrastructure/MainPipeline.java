package com.amazon.pipeline.infrastructure;

import com.amazon.pipeline.application.CleanSalesUseCase;
import com.amazon.pipeline.domain.FieldMetadata;
import com.amazon.pipeline.domain.SaleRepository;
import com.amazon.pipeline.infrastructure.config.ConfigLoader;
import com.amazon.pipeline.infrastructure.persistence.mongo.MetadataLoader;
import com.amazon.pipeline.infrastructure.persistence.mongo.MongoGenericAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

@Slf4j
public class MainPipeline {

    public static void main(String[] args) {
        Properties props = ConfigLoader.load();

        String[] customArgs = {
                "--inputFile=" + props.getProperty("input.file"),
                "--metadataConfigJson=" + props.getProperty("metadata.path"),
                "--mongoUri=" + props.getProperty("mongo.uri"),
                "--database=" + props.getProperty("mongo.db"),
                "--collection=" + props.getProperty("mongo.collection")
        };

        String[] finalArgs = Stream.concat(Arrays.stream(args), Arrays.stream(customArgs))
                .toArray(String[]::new);

        jsonPipelineOptions options = PipelineOptionsFactory.fromArgs(finalArgs)
                .withValidation()
                .as(jsonPipelineOptions.class);

        configureLocalRunner();

        Pipeline p = Pipeline.create(options);

        List<FieldMetadata> metadata = MetadataLoader.load(options.getMetadataConfigJson());

        SaleRepository repository = new MongoGenericAdapter(
                options.getMongoUri(),
                options.getDatabase(),
                options.getCollection()
        );

        CleanSalesUseCase useCase = new CleanSalesUseCase(repository, metadata);

        // Definition of the Beam Graph
        PCollection<String> input = p.apply("ReadCSV",
                TextIO.read().from(options.getInputFile()));

        useCase.execute(input);

        log.info(">>> Starting Pipeline execution...");
        PipelineResult result = p.run();
        result.waitUntilFinish();

        // Metrics
        printFinalMetrics(result);

        log.info(">>> Pipeline execution complete. Shutting down.");
        System.exit(0);
    }

    private static void configureLocalRunner() {
        // Specific settings to prevent DirectRunner from blocking
        System.setProperty("beamDirectRunnerMaxBundleSize", "10000");
        System.setProperty("targetParallelism", "1");
    }

    private static void printFinalMetrics(PipelineResult result) {
        try {
            MetricQueryResults metrics = result.metrics().queryMetrics(MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("CleanTransform", "processed_sales"))
                    .addNameFilter(MetricNameFilter.named("Sales", "unique_count"))
                    .addNameFilter(MetricNameFilter.named("Audit", "total_records_sent"))
                    .build());

            for (MetricResult<Long> counter : metrics.getCounters()) {
                log.info(">>> METRIC | {} : {}", counter.getName().getName(), counter.getAttempted());
            }
        } catch (Exception e) {
            log.warn(">>> Could not retrieve metrics: {}", e.getMessage());
        }
    }
}