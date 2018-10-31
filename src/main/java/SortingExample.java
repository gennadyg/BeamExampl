//import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public class SortingExample {

    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.read().from("c:\\IdeaProjects\\BeamExample\\input\\1q84"))
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split( TOKENIZER_PATTERN)) {
                            if (word.length() > 1) {
                                c.output(word.toLowerCase());
                            }
                        }
                    }
                }))
                .apply(Count.perElement())
                .apply("CreateKey", ParDo.of(new DoFn<KV<String, Long>, KV<String, KV<String, Long>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Long> element = c.element();
                        String key = element.getKey();
                        c.output(KV.of("single", KV.of(key, element.getValue())));
                    }
                }))
                .apply(GroupByKey.create())
                .apply("FormatResults",
                        MapElements.via(
                                new SimpleFunction<KV<String, Iterable<KV<String, Long>>>, String>() {
                                    @Override
                                    public String apply(KV<String, Iterable<KV<String, Long>>> input) {
                                        return StreamSupport.stream(input.getValue().spliterator(), false)
                                                .collect((Supplier<ArrayList<KV<String, Long>>>) ArrayList::new,
                                                        (al, kv) -> al.add(KV.of(kv.getKey(), kv.getValue())),
                                                        (sb, kv) -> {
                                                        })
                                                .stream()
                                                .sorted((kv1, kv2) -> kv2.getKey().compareTo(kv1.getKey()))
                                                .collect(StringBuilder::new,
                                                        (sb, kv) -> sb.append(String.format("%20s : %d%n", kv.getKey(), kv.getValue())),
                                                        (sb, kv) -> {
                                                        }).toString();
                                    }
                                }
                        ))
                .apply(TextIO.write().withNumShards(1).to("minimal-wordcount-bible"));
        p.run().waitUntilFinish();
    }
}