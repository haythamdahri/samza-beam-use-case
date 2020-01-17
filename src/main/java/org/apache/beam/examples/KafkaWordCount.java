package org.apache.beam.examples;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

/**
 * @author Haytham DAHRI
 * Cet exmple permet de compter les mots à partir d'un stream (Flux) Kafka
 *
 * <pre>
 *   1. Lire les données à partir d'un Topic (Sujet) Kafka
 *   2. Specifier les transformation
 *   3. Assign a window Affecter une fenêtre (Window)
 *   4. Compter les items dans une PCollection
 *   5. Écrire les données dans un output kakfa topic (Sujet de sortie kafka)
 * </pre>
 *
 * <p>Créer input topic avant l'exécution:
 * <pre>{@code
 * $ ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic input-text --partitions 10 --replication-factor 1
 * }</pre>
 *
 * <p>Pour exécuter locallement:
 * <pre>{@code
 * $ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.KafkaWordCount \
 * -Dexec.args="--runner=SamzaRunner" -P samza-runner
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
 * <pre>{@code
 * $ deploy/examples/bin/run-beam-standalone.sh org.apache.beam.examples.KafkaWordCount --configFilePath=$PWD/deploy/examples/config/standalone.properties --maxSourceParallelism=1024
 * }</pre>
 *
 * <p>Pour exécuter avec yarn:
 * <pre>{@code
 * $ deploy/examples/bin/run-beam-yarn.sh org.apache.beam.examples.KafkaWordCount --configFilePath=$PWD/deploy/examples/config/yarn.properties --maxSourceParallelism=1024
 * }</pre>
 *
 * <p>Pour produire des données de test:
 * <pre>{@code
 * $ ./deploy/kafka/bin/kafka-console-producer.sh --topic input-text --broker-list localhost:9092 <br/>
 * Le big data a une histoire récente et pour partie cachée, en tant qu'outil des technologies de l'information et comme espace virtuel prenant une importance volumique croissante dans le cyberespace.
 * }</pre>
 *
 * <p>Pour verifier output:
 * <pre>{@code
 * $ ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count --property print.key=true
 * }</pre>
 *
 */
public class KafkaWordCount {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    options.setJobName("kafka-word-count");

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    p.apply(
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("input-text")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withoutMetadata())
        .apply(Values.create())
        // Appliquer a FlatMapElements pour transformer les lignes de texte de PCollection.
        // cette methode permet de trandformer les splits de lines dans PCollection<String>, où chaque element est un mot
        // individuel dans le texte collecté.
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
        // On utilise un Filtre de transformation pour éviter les mots vides
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        // Appliquer la transformation de comptage vers notre PCollection des mots individuels. 
        // Le comptage de tranformation retourne une nouvelle PCollection des paires key/value, chaque clé représente un mot unique
        // dans le texte. La valeur asscoiée est le nombre d'occurrence de ce mot.
        .apply(Count.perElement())
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(kv -> KV.of(kv.getKey(), String.valueOf(kv.getValue()))))
        .apply(KafkaIO.<String, String>write()
            .withBootstrapServers("localhost:9092")
            .withTopic("word-count")
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class));

    // Pour yarn, nous devons pas attendre aprés que le Jo soit soumis.
    // Aucune utilitée d'utiliser waitUntilFinish(). On doit utiliser p.run()
    p.run().waitUntilFinish();
  }
}
