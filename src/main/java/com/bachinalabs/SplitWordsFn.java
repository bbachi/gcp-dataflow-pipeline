package com.bachinalabs;

import org.apache.beam.sdk.transforms.DoFn;

public class SplitWordsFn extends DoFn<String, String> {

    public static final String SPLIT_PATTERN = ":";

    @ProcessElement
    public void processElement(ProcessContext c) {
        for(String word: c.element().split(SPLIT_PATTERN)) {
            if (!word.isEmpty()) {
                System.out.println(word);
                c.output(word);
            }
        }
    }

}
