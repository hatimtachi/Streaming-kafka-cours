package fr.esgi.kafka.tp14;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * TP14 - Interactive Queries + Tests  (STARTER : completez le TODO d'IQ).
 *
 * Agrege les commandes "orderId;amount;currency" PAR DEVISE (count) et expose
 * une petite API REST : GET http://localhost:7070/count/EUR
 * La reponse vient DIRECTEMENT du state store (Interactive Query), sans relire de topic.
 *
 *   mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp14.CountApiApp
 *   mvn -q test        # execute les tests TopologyTestDriver (sans cluster)
 */
public class CountApiApp {

    static final String BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092";
    static final String STORE = "orders-count-by-currency-store";
    static final int API_PORT = 7070;

    /** Topologie : compter les commandes par devise. Reutilisee par les tests. */
    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> orders =
                builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()))
                       .filter((k, v) -> isValid(v));
        orders.groupBy((k, v) -> currencyOf(v), Grouped.with(Serdes.String(), Serdes.String()))
              .count(Materialized.as(STORE))
              .toStream()
              .mapValues(v -> Long.toString(v))
              .to("count-by-currency", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tp14-count-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put("auto.offset.reset", "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        KafkaStreams streams = new KafkaStreams(buildTopology(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        startApi(streams);
        System.out.println("Agregateur + API : http://localhost:" + API_PORT + "/count/{devise}");
    }

    /** Expose GET /count/DEVISE en interrogeant le store (Interactive Query). */
    static void startApi(KafkaStreams streams) throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress(API_PORT), 0);
        http.createContext("/count/", exchange -> {
            String ccy = exchange.getRequestURI().getPath()
                                 .substring("/count/".length()).toUpperCase();
            int code = 200;
            String body;
            try {
                Long n = null;
                // TODO : Interactive Query
                //   - Recuperer le state store en LECTURE SEULE depuis 'streams'
                //     (voir streams.store(...) + StoreQueryParameters.fromNameAndType,
                //      avec le nom STORE et le type QueryableStoreTypes.keyValueStore()).
                //   - Lire le compteur courant de la devise 'ccy' et l'affecter a 'n'.
                //   (si le store n'est pas encore pret, l'exception ci-dessous renvoie 503)
                body = ccy + " = " + (n == null ? 0 : n);
            } catch (InvalidStateStoreException e) {
                code = 503;   // store momentanement indisponible (rebalance) -> reessayer
                body = "store indisponible (rebalance), reessayez";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(code, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        http.start();
    }

    static boolean isValid(String line) {
        String[] p = line.split(";");
        if (p.length < 3) return false;
        try {
            Double.parseDouble(p[1]);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static String currencyOf(String line) {
        String[] p = line.split(";");
        return (p.length >= 3) ? p[2].toUpperCase() : "?";
    }
}
