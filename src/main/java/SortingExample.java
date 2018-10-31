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
                .apply("CreateKey", ParDo.of(new DoFn<KV<String, Long>, KV<String, KV<Long, String>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Long> element = c.element();
                        String key = element.getKey();
                        c.output(KV.of("single", KV.of(element.getValue(), key )));
                    }
                }))
                .apply(GroupByKey.create())
                .apply("FormatResults",
                        MapElements.via(
                                new SimpleFunction<KV<String, Iterable<KV<Long, String>>>, String>() {
                                    @Override
                                    public String apply(KV<String, Iterable<KV<Long, String>>> input) {
                                        return StreamSupport.stream(input.getValue().spliterator(), false)
                                                .collect((Supplier<ArrayList<KV< Long, String>>>) ArrayList::new,
                                                        (al, kv) -> al.add(KV.of(kv.getKey(), kv.getValue())),
                                                        (sb, kv) -> {
                                                        })
                                                .stream()
                                                .sorted((kv1, kv2) -> kv2.getKey().compareTo(kv1.getKey()))
                                                .collect(StringBuilder::new,
                                                        (sb, kv) -> sb.append(String.format("%d : %20s%n", kv.getKey(), kv.getValue())),
                                                        (sb, kv) -> {
                                                        }).toString();
                                    }
                                }
                        ))
                .apply(TextIO.write().withNumShards(1).to("c:\\IdeaProjects\\BeamExample\\input\\1q84_sorted_by_counts"));
        p.run().waitUntilFinish();
    }
}