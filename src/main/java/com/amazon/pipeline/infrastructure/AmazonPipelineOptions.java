package com.amazon.pipeline.infrastructure;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface AmazonPipelineOptions extends PipelineOptions {
    @Description("Path del archivo CSV de entrada")
    @Default.String("src/main/resources/amazon_sales.csv")
    String getInputFile();
    void setInputFile(String value);

    @Description("URI de conexión a MongoDB")
    @Default.String("mongodb://localhost:27017")
    String getMongoUri();
    void setMongoUri(String value);

    @Description("Nombre de la base de datos")
    @Default.String("amazon_data")
    String getDatabase();
    void setDatabase(String value);

    @Description("Nombre de la colección")
    @Default.String("sales")
    String getCollection();
    void setCollection(String value);
}
