package com.bachinalabs;

import org.apache.beam.sdk.transforms.DoFn;

public class ExtractWordsFn extends DoFn<String, String> {

    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    @ProcessElement
    public void processElement(ProcessContext c) {
        for(String word: c.element().split(TOKENIZER_PATTERN)) {
            if (!word.isEmpty()) {
                System.out.println(word);
                c.output(word);
            }
        }
    }
}
