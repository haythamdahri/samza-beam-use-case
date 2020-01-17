package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * An example that counts words and includes Beam best practices.
 *
 * <p>For a detailed walkthrough of this example, see <a
 * href="https://beam.apache.org/get-started/wordcount-example/">
 * https://beam.apache.org/get-started/wordcount-example/ </a>
 *
 * <p>Basic concepts: Reading text files; counting a
 * PCollection; writing to text files
 *
 * <p>New Concepts:
 *
 * <pre>
 *   1. Exécuter le Pipeline locallement et en utilisant le runner utilisé
 *   2. Ulisation de ParDo DoFns statique défini out-of-line
 *   3. Construction d'un composant de transformation
 *   4. Definition de nos propres options de pipeline 
 * </pre>
 *
 * <p>Pour exécuter locallement:
 * <pre>{@code
 * $ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
 *      -Dexec.args="--inputFile=pom.xml --output=counts --runner=SamzaRunner" -Psamza-runner
 * }</pre>
 *
 * <p>Pour executer l'étude de cas dans un environnement distribué, utiliser mvn pour empaqueter le projet:
 * (supprimer .waitUntilFinish() dans le code pour le déploiement de yarn)
 * <pre>{@code
 * $ mkdir -p deploy/examples
 * $ mvn package && tar -xvf target/samza-beam-examples-0.1-dist.tar.gz -C deploy/examples/
 * }</pre>
 *
 * <p>Pour exécuter en autonomie avec zookeeper:
 * (split the input by 2)
 * <pre>{@code
 * $ deploy/examples/bin/run-beam-standalone.sh org.apache.beam.examples.WordCount \
 *     --configFilePath=$PWD/deploy/examples/config/standalone.properties \
 *     --inputFile=/Users/xiliu/opensource/samza-beam-examples/pom.xml --output=word-counts.txt \
 *     --maxSourceParallelism=2
 * }</pre>
 *
 * <p>Pour exécuter avec yarn:
 * <pre>{@code
 * $ deploy/examples/bin/run-beam-yarn.sh org.apache.beam.examples.WordCount \
 *     --configFilePath=$PWD/deploy/examples/config/yarn.properties \
 *     --inputFile=/Users/xiliu/opensource/samza-beam-examples/pom.xml \
 *     --output=/tmp/word-counts.txt --maxSourceParallelism=2
 * }</pre>
 */
public class WordCount {
  private static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

  static class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
        Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      // Split la ligne en mots.
      String[] words = element.split(TOKENIZER_PATTERN, -1);

      // Sortir chaque mot vers output de PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  /** Une fonctionne simple qui convertie un mot et comptages en chaines de caractères affichable. */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class CountWords
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
    }
  }

  public interface WordCountOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Required
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);
  }

  static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(new CountWords())
        .apply(MapElements.via(new FormatAsTextFn()))
        .apply("WriteCounts", TextIO.write().to(options.getOutput()).withoutSharding());

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
    options.setJobName("word-count");

    runWordCount(options);
  }
}
