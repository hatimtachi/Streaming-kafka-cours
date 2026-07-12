package fr.esgi.kafka.tp7;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * TP S7 - Consommateur Avro a completer.
 *
 * Lit les commandes "Order" du topic "orders" et les decode via le Schema
 * Registry : le deserialiseur lit l'ID de schema dans le message, recupere le
 * schema correspondant, et reconstruit un GenericRecord.
 *
 * Usage   : ./run.sh consumer
 * Solution: ./run.sh consumer solution
 */
public class AvroOrderConsumer {

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // TODO 3a - deserialiseur de VALEUR Avro :
        //
        // TODO 3b - adresse du registre + lecture en GenericRecord :
        //
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumer = new KafkaConsumer<String, GenericRecord>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of("orders"));
            System.out.println("Consommateur Avro (a completer). Ctrl-C pour arreter...");
            while (true) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, GenericRecord> r : records) {
                    var o = r.value();
                    // TODO 3c - afficher les champs decodes (orderId, amount, currency) ;
                    //   "status" n'existe que dans les messages v2 -> garder le test :
                    //
                    System.out.println("recu (non decode) key=" + r.key());
                }
            }
        } catch (WakeupException e) {
            // arret demande
        } finally {
            consumer.close();
            System.out.println("Consommateur arrete.");
        }
    }
}
