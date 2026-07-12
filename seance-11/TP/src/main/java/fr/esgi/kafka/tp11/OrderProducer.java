package fr.esgi.kafka.tp11;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * TP S11 - Producteur de demonstration (FOURNI, rien a completer).
 *
 * Format d'une ligne : orderId;amount;currency
 * Plusieurs devises, plusieurs montants, pour voir les agregats evoluer.
 *
 * Resultats attendus (apres agregation par devise) :
 *   - count-by-currency : EUR=3, USD=3, GBP=1
 *   - total-by-currency : EUR=69.90, USD=155.00, GBP=12.00
 *
 * Lancer : ./run.sh seed        (ou .\run.ps1 seed)
 * Relancer plusieurs fois : les totaux CUMULENT (l'etat persiste).
 */
public class OrderProducer {

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var orders = List.of(
            "o-1;19.9;eur",
            "o-2;5.0;usd",
            "o-3;42.0;eur",
            "o-4;100.0;usd",
            "o-5;12.0;gbp",
            "o-6;8.0;eur",
            "o-7;50.0;usd");

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (var line : orders) {
                var key = line.split(";")[0];
                producer.send(new ProducerRecord<>("orders", key, line));
                System.out.println("produit -> " + line);
            }
            producer.flush();
        }
        System.out.println("Produit " + orders.size() + " commandes dans 'orders' "
                + "(EUR x3 = 69.90, USD x3 = 155.00, GBP x1 = 12.00).");
    }
}
