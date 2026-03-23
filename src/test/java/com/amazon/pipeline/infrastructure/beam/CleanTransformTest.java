package com.amazon.pipeline.infrastructure.beam;

import com.amazon.pipeline.domain.AmazonSale;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.testng.annotations.Test;

public class CleanTransformTest {
    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
    void testPipelineTransform() {
        PCollection<String> input = p.apply(Create.of("1,ORD123,2024,150.50,Home"));
        PCollection<AmazonSale> output = input.apply(new CleanTransform());

        PAssert.that(output).containsInAnyOrder(new AmazonSale("ORD123", 150.50, "Home"));
        p.run();
    }
}
