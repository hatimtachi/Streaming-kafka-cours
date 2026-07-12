package fr.esgi.kafka.tp9;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * TP S9 - Producteur de demonstration (FOURNI, rien a completer).
 *
 * Envoie quelques commandes "orderId;amount;currency" dans le topic "orders" :
 *   - des montants < 10 (qui seront filtres par la topologie) ;
 *   - des devises en minuscules (qui seront mises en majuscules).
 *
 * Lancer : ./run.sh seed        (ou .\run.ps1 seed)
 */
public class OrderProducer {

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var orders = List.of(
            "o-1;19.9;eur",
            "o-2;5.0;usd",     // < 10 -> filtre
            "o-3;42.0;gbp",
            "o-4;7.5;eur",     // < 10 -> filtre
            "o-5;100.0;usd",
            "o-6;12.0;chf");

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (var line : orders) {
                var key = line.split(";")[0];
                producer.send(new ProducerRecord<>("orders", key, line));
                System.out.println("produit -> " + line);
            }
            producer.flush();
        }
        System.out.println("Produit " + orders.size() + " commandes dans 'orders' "
                + "(attendu en sortie : 4, devises en majuscules).");
    }
}
