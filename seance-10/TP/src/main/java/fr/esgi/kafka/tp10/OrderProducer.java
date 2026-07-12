package fr.esgi.kafka.tp10;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * TP S10 - Producteur de demonstration (FOURNI, rien a completer).
 *
 * Format d'une ligne : orderId;amount;currency;sku1|sku2|...
 * Le jeu contient volontairement DEUX lignes malformees (a router vers
 * orders-rejected) et un melange de montants (>= 100 = prioritaire).
 *
 * Resultats attendus :
 *   - orders-priority : o-1, o-3                (montant >= 100)   -> 2
 *   - orders-standard : o-2, o-4, o-6           (montant < 100)    -> 3
 *   - order-items     : A,B,D,E,F,C,G,I,J                          -> 9
 *   - orders-rejected : la ligne sans ';' et o-5 (montant invalide) -> 2
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
            "o-1;150.0;eur;A|B",        // prioritaire, 2 articles
            "o-2;30.0;usd;C",           // standard, 1 article
            "o-3;500.0;gbp;D|E|F",      // prioritaire, 3 articles
            "o-4;12.0;eur;G",           // standard, 1 article
            "ligne-sans-separateur",    // MALFORMEE -> rejected
            "o-5;pasunnombre;usd;H",    // montant invalide -> rejected
            "o-6;99.99;chf;I|J");       // standard (< 100), 2 articles

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (var line : orders) {
                var key = line.split(";")[0];
                producer.send(new ProducerRecord<>("orders", key, line));
                System.out.println("produit -> " + line);
            }
            producer.flush();
        }
        System.out.println("Produit " + orders.size() + " commandes dans 'orders' "
                + "(2 prioritaires, 3 standard, 9 articles, 2 rejets).");
    }
}
