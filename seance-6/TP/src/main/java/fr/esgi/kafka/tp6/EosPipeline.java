package fr.esgi.kafka.tp6;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * TP S6 - Pipeline exactly-once a completer (consume-transform-produce).
 *
 *   orders  --(lire valide)-->  [transform]  --(transaction)-->  orders-validated
 *
 * On lit "orders", on transforme, et on reproduit vers "orders-validated" DE FACON
 * ATOMIQUE (sortie + offsets dans une meme transaction). Un message empoisonne
 * doit declencher abortTransaction().
 *
 * Lancer   : ./run.sh pipeline        (ou .\run.ps1 pipeline)
 * Solution : ./run.sh pipeline solution    (package fr.esgi.kafka.tp6.solution)
 *
 * Les helpers (toValidated, nextOffset, PoisonException) sont fournis en bas.
 */
public class EosPipeline {

    static final String IN = "orders";
    static final String OUT = "orders-validated";
    static final String GROUP = "eos-pipeline";

    public static void main(String[] args) {
        // --- Consommateur ---
        var cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // TODO 1 - lecture exactly-once cote consommateur :


        // --- Producteur transactionnel ---
        var pProps = new Properties();
        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // TODO 2a - identifier le producteur transactionnel (implique l'idempotence) :

        var consumer = new KafkaConsumer<String, String>(cProps);
        var producer = new KafkaProducer<String, String>(pProps);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        // TODO 2b - initialiser les transactions UNE fois :

        try {
            consumer.subscribe(List.of(IN));
            System.out.println("Pipeline a completer (voir les TODO). " + IN + " -> " + OUT);
            while (true) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (var r : records) {
                    // TODO 3 - envelopper le traitement d'un message dans une transaction :

                    System.out.printf("recu (non traite) p=%d off=%d key=%s%n", r.partition(), r.offset(), r.key());
                }
            }
        } catch (WakeupException e) {
            // arret demande
        } finally {
            producer.close();
            consumer.close();
            System.out.println("Pipeline arrete.");
        }
    }

    // ---- Helpers fournis ----

    /** Transforme une commande validee ; rejette le message empoisonne. */
    static ProducerRecord<String, String> toValidated(ConsumerRecord<String, String> r) {
        if ("POISON".equals(r.value())) {
            throw new PoisonException();
        }
        return new ProducerRecord<>(OUT, r.key(), "validated:" + r.value());
    }

    /** Offset du PROCHAIN message a lire sur la partition de r. */
    static Map<TopicPartition, OffsetAndMetadata> nextOffset(ConsumerRecord<String, String> r) {
        return Map.of(new TopicPartition(r.topic(), r.partition()),
                new OffsetAndMetadata(r.offset() + 1));
    }

    static class PoisonException extends RuntimeException {}
}
