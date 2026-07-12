package app;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * TP S4 - Consommateur a completer.
 *
 * Objectif : s'abonner au topic "commandes" et afficher, pour chaque message,
 *            sa partition, son offset et sa valeur, dans une boucle poll.
 *
 * Lancer  :  ./run.sh consumer        (ou .\run.ps1 consumer)
 *            mvn -q compile exec:java -Dexec.mainClass=app.ConsumerApp
 * Solution:  package "solution" (./run.sh consumer solution)
 *
 * Arret : Ctrl-C.
 */
public class ConsumerApp {

    public static void main(String[] args) {
        var props = new Properties();

        // TODO 1 - Configurer le consommateur (props.put(...)) :
        //   - "bootstrap.servers"  = "localhost:9092"
        //   - "key.deserializer"   = StringDeserializer.class.getName()
        //   - "value.deserializer" = StringDeserializer.class.getName()
        //   - "group.id"           = "atelier-s4"
        //   - "auto.offset.reset"  = "earliest"

        // TODO 2 - Creer le consommateur dans un try-with-resources :
        //   try (var consumer = new KafkaConsumer<String, String>(props)) {
        //       consumer.subscribe(List.of("commandes"));
        //       while (true) {
        //           var records = consumer.poll(Duration.ofMillis(500));
        //           for (var r : records) { ... afficher p / offset / valeur ... }
        //       }
        //   }

        System.out.println("ConsumerApp a completer : voir les TODO.");
    }
}
