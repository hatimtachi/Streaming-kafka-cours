package fr.esgi.kafka.tp9;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * TP S9 - Premiere topologie Kafka Streams (a completer).
 *
 *   orders  --(filter montant >= 10)-->  (mapValues : devise en MAJUSCULES)  -->  orders-filtered
 *
 * Kafka Streams est une BIBLIOTHEQUE : cette classe EST l'application. On la
 * lance comme un programme normal ; elle tourne en continu jusqu'a Ctrl-C.
 * Pour passer a l'echelle, on lance plusieurs instances avec le MEME
 * application.id (elles forment un consumer group).
 *
 * Lancer   : ./run.sh app           (ou .\run.ps1 app)
 * Solution : ./run.sh app solution
 *
 * Les helpers amountOf(...) et upperCurrency(...) sont fournis en bas.
 */
public class OrderStreamApp {

    static final String IN = "orders";
    static final String OUT = "orders-filtered";

    public static void main(String[] args) {
        // --- Configuration ---
        var props = new Properties();
        // TODO 1 - completer la configuration :
        //
        // --- Topologie ---
        var builder = new StreamsBuilder();
        KStream<String, String> orders =
            builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()));

        // TODO 2 - filtrer (montant >= 10) puis normaliser la devise (majuscules) :
        //
        // TODO 3 - ecrire le resultat dans OUT (et enlever la ligne ci-dessous) :
        //

        var streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        System.out.println("(a completer) topologie " + IN + " -> " + OUT);
        streams.start();
    }

    // ---- Helpers fournis ----

    /** Montant d'une ligne "orderId;amount;currency" (0 si illisible -> sera filtre). */
    static double amountOf(String line) {
        try {
            return Double.parseDouble(line.split(";")[1]);
        } catch (Exception e) {
            return 0.0;
        }
    }

    /** Met la devise en majuscules : "o-1;19.9;eur" -> "o-1;19.9;EUR". */
    static String upperCurrency(String line) {
        var parts = line.split(";");
        if (parts.length < 3) return line;
        return parts[0] + ";" + parts[1] + ";" + parts[2].toUpperCase();
    }
}
