package com.amazon.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface AmazonPipelineOptions extends PipelineOptions {
    @Description("Path del archivo CSV de entrada")
    @Default.String("src/main/resources/AmazonSaleReport.csv")
    ValueProvider<String> getInputFile();
    void setInputFile(ValueProvider<String> value);

    @Description("URL de la base de datos")
    @Default.String("jdbc:postgresql://localhost:5432/amazon_sales")
    String getJdbcUrl();
    void setJdbcUrl(String value);
}
