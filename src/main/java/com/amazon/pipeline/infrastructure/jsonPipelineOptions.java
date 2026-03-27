package com.amazon.pipeline.infrastructure;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface jsonPipelineOptions extends PipelineOptions {
    @Description("Input CSV file path")
    @Validation.Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Path to the metadata JSON file")
    @Validation.Required
    String getMetadataConfigJson();
    void setMetadataConfigJson(String value);

    @Description("MongoDB connection URI")
    String getMongoUri();
    void setMongoUri(String value);

    @Description("Database name")
    String getDatabase();
    void setDatabase(String value);

    @Description("Collection name")
    String getCollection();
    void setCollection(String value);
}
