package com.amazon.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface AmazonPipelineOptions extends PipelineOptions {
    @Description("Input CSV file path")
    @Default.String("src/main/resources/AmazonSaleReport.csv")
    ValueProvider<String> getInputFile();
    void setInputFile(ValueProvider<String> value);
}
