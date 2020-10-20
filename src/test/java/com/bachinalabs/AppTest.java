package com.bachinalabs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    // Create a line for the input
    static final String line = "test:test:test:testing in progress:testing in progress:testing completed:done";


    public void testCountWords() throws Exception {

        Pipeline p = TestPipeline.create();

        // Reading line and convert it into PCollection of Strings
        PCollection<String> input = p.apply(Create.of(line));

        // Run the SplitWords Transform
        PCollection<String> splitWordsOutput = input.apply(new SplitWords());

        // Run the CountWords Transform
        PCollection<KV<String, Long>> countWordsOutput = splitWordsOutput.apply(new CountWords());

        // Assert that the output PCollection matches the COUNTS_ARRAY known static output data.
        PAssert.that(countWordsOutput).containsInAnyOrder(KV.of("test", 3L),
                KV.of("testing", 3L),
                KV.of("progress", 2L),
                KV.of("done", 1L),
                KV.of("completed", 1L),
                KV.of("in", 2L));

        // Run the Pipeline
        p.run();
    }
}
