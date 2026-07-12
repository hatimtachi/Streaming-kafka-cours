package fr.esgi.kafka.tp6;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TP S6 - Seeder (FOURNI, rien a completer).
 *
 * Alimente le topic "orders" avec 10 commandes, dont UNE empoisonnee
 * (valeur "POISON" a l'index 6) pour tester l'abort dans le pipeline.
 *
 * Producteur idempotent par defaut en Kafka 4.x (acks=all + enable.idempotence).
 *
 * Lancer : ./run.sh seed        (ou .\run.ps1 seed)
 */
public class OrderSeeder {

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < 10; i++) {
                var key = "cmd-" + i;
                var value = (i == 6) ? "POISON" : ("amount=" + (10 * (i + 1)));
                producer.send(new ProducerRecord<>("orders", key, value));
                System.out.printf("seed -> key=%s value=%s%n", key, value);
            }
            producer.flush();
        }
        System.out.println("orders alimente : 10 commandes (dont 1 empoisonnee a l'index 6).");
    }
}
