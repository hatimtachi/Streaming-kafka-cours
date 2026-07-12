package fr.esgi.kafka.tp7;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.util.Properties;

/**
 * TP S7 - Producteur Avro a completer.
 *
 * Produit des commandes "Order" en Avro vers le topic "orders". La VALEUR est
 * serialisee avec KafkaAvroSerializer : le schema est enregistre automatiquement
 * dans le Schema Registry (sujet "orders-value"), et seul l'ID + le payload
 * partent sur le fil.
 *
 * Usage   : ./run.sh producer            (schema v1)
 *           ./run.sh producer v2         (schema v2, ajoute "status")
 * Solution: ./run.sh producer solution [v1|v2]
 *
 * Le helper loadSchema(...) est fourni (charge un .avsc du classpath).
 */
public class AvroOrderProducer {

    public static void main(String[] args) throws Exception {
        var version = (args.length > 0) ? args[0] : "v1";          // "v1" ou "v2"
        var schema = loadSchema("/avro/order-" + version + ".avsc");

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // TODO 1a - serialiseur de VALEUR Avro :
        //
        // TODO 1b - adresse du registre :
        //

        try (var producer = new KafkaProducer<String, GenericRecord>(props)) {
            for (int i = 0; i < 5; i++) {
                // TODO 1c - construire un GenericRecord conforme au schema :
                //
                System.out.println("TODO : produire la commande cmd-" + i + " en Avro (" + version + ")");
            }
            producer.flush();
        }
        System.out.println("(a completer) producteur Avro " + version);
    }

    /** Charge un schema Avro depuis le classpath (src/main/resources). */
    static Schema loadSchema(String resource) throws Exception {
        try (InputStream in = AvroOrderProducer.class.getResourceAsStream(resource)) {
            if (in == null) throw new IllegalStateException("Schema introuvable : " + resource);
            return new Schema.Parser().parse(in);
        }
    }
}
