package fr.esgi.kafka.tp13;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.Properties;

/**
 * TP13 - Donnees en retard, grace period et horodatages  (STARTER : completez les TODO).
 *
 * Entree "orders-ts" : lignes "orderId;amount;currency;eventTimeMillis".
 * L'event time est PORTE PAR LE MESSAGE (4e champ) -> on injecte des retards de
 * facon DETERMINISTE, independamment de l'heure reelle.
 *
 * Sortie "windowed-counts" : nb de commandes par devise et par fenetre (10 s),
 * emis UNE seule fois a la fermeture de la fenetre (suppress).
 *
 *   mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp13.LateDataApp
 */
public class LateDataApp {

    static final String BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092";
    static final String INPUT = "orders-ts";
    static final String OUTPUT = "windowed-counts";

    /** Extrait l'event time du 4e champ "orderId;amount;currency;eventTimeMillis". */
    static class EventTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            Object v = record.value();
            if (v != null) {
                String[] p = v.toString().split(";");
                if (p.length >= 4) {
                    try {
                        long ts = Long.parseLong(p[3].trim());
                        // TODO 1 : l'event time est le 4e champ, deja parse dans 'ts'.
                        //   S'il est valide (positif), le RENVOYER ici pour qu'il serve
                        //   d'event time. Sinon, ne rien renvoyer : le repli
                        //   'partitionTime' (plus bas) s'appliquera.
                    } catch (NumberFormatException e) {
                        // valeur non numerique -> repli
                    }
                }
            }
            return partitionTime;   // repli : derniere heure connue de la partition
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tp13-late-data");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put("auto.offset.reset", "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> orders = builder.stream(INPUT,
                        Consumed.with(Serdes.String(), Serdes.String())
                                .withTimestampExtractor(new EventTimeExtractor()))
                .filter((k, v) -> isValid(v));

        // TODO 2 : comptage par fenetre + resultat FINAL uniquement  -> topic OUTPUT
        //   - Grouper le flux par devise (groupBy, en s'aidant de currencyOf).
        //   - Fenetrer en fenetres de 10 s avec 5 s de grace (voir TimeWindows).
        //   - Compter par cle et par fenetre.
        //   - N'emettre QUE le resultat final de chaque fenetre, a sa fermeture
        //     (voir suppress(...) / untilWindowCloses).
        //   - Repasser en flux, convertir la cle Windowed en "devise@debutFenetre",
        //     et ecrire dans le topic OUTPUT ("windowed-counts").

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        System.out.println("Late-data app demarree (app.id=tp13-late-data). Ctrl-C pour arreter.");
    }

    /** Vrai si la ligne a 4 champs, un montant numerique et un horodatage numerique. */
    static boolean isValid(String line) {
        String[] p = line.split(";");
        if (p.length < 4) return false;
        try {
            Double.parseDouble(p[1]);
            Long.parseLong(p[3].trim());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Devise (3e champ) en majuscules. */
    static String currencyOf(String line) {
        String[] p = line.split(";");
        return (p.length >= 3) ? p[2].toUpperCase() : "?";
    }
}
