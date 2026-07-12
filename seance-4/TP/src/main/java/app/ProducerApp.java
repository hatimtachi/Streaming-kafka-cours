package app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TP S4 - Producteur a completer.
 *
 * Objectif : envoyer 10 messages dans le topic "commandes",
 *            avec une cle "client-i" et une valeur "montant=i".
 *
 * Lancer  :  ./run.sh producer        (ou .\run.ps1 producer)
 *            mvn -q compile exec:java -Dexec.mainClass=app.ProducerApp
 * Solution:  package "solution" (./run.sh producer solution)
 */
public class ProducerApp {

    public static void main(String[] args) {
        var props = new Properties();

        // TODO 1 - Configurer le producteur (props.put(...)) :
        //   - "bootstrap.servers" = "localhost:9092"
        //   - "key.serializer"    = StringSerializer.class.getName()
        //   - "value.serializer"  = StringSerializer.class.getName()
        //   - "acks"              = "all"

        // TODO 2 - Creer le producteur dans un try-with-resources :
        //   try (var producer = new KafkaProducer<String, String>(props)) {
        //       ... boucle de 0 a 9 ...
        //   }
        //
        // TODO 3 - Pour chaque i, construire un ProducerRecord<>(
        //              "commandes", "client-" + i, "montant=" + i)
        //          et l'envoyer avec producer.send(record, callback).
        //          Le callback affiche meta.partition() et meta.offset().

        System.out.println("ProducerApp a completer : voir les TODO.");
    }
}
