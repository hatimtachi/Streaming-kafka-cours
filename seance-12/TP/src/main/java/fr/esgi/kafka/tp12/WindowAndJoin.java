package fr.esgi.kafka.tp12;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

/**
 * TP12 - Fenetrage et jointures  (STARTER : completez les TODO).
 *
 * Entrees :
 *   - "orders" : flux "orderId;amount;currency"
 *   - "rates"  : table de reference, cle = devise, valeur = taux (ex. EUR -> 1.0)
 * Sorties :
 *   - "orders-windowed-count" : nb de commandes par devise et par fenetre (1 min)
 *   - "orders-converted"      : chaque commande enrichie du montant converti
 *
 *   mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp12.WindowAndJoin
 */
public class WindowAndJoin {

    static final String BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tp12-window-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put("auto.offset.reset", "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> orders =
                builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()))
                       .filter((k, v) -> isValid(v));

        // TODO 1 : la table de reference et la cle commune
        //   - Lire le topic "rates" comme une KTable (cle = devise, valeur = taux).
        //   - Rekeyer le flux "orders" par devise (selectKey, en s'aidant de currencyOf)
        //     et CONSERVER ce flux dans une variable : il servira a la fois au
        //     fenetrage (TODO 2) et a la jointure (TODO 3).

        // TODO 2 : le comptage par fenetre  -> topic "orders-windowed-count"
        //   - Grouper le flux rekeye par cle (groupByKey).
        //   - Fenetrer en fenetres d'UNE MINUTE avec 10 s de grace (voir TimeWindows).
        //   - Compter les enregistrements par cle et par fenetre.
        //   - Repasser en flux (toStream) : la cle du resultat est un Windowed<String>
        //     (une devise + une fenetre) ; la convertir en chaine "devise@debutFenetre".
        //   - Ecrire dans le topic "orders-windowed-count".

        // TODO 3 : l'enrichissement par jointure  -> topic "orders-converted"
        //   - Joindre le flux rekeye par devise avec la KTable "rates"
        //     (jointure KStream-KTable, non fenetree : un lookup du taux courant).
        //   - Le ValueJoiner recoit (ligne de commande, taux) ; la methode convert(...)
        //     fournie calcule la ligne enrichie.
        //   - Ecrire le resultat dans le topic "orders-converted".

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        System.out.println("Topologie demarree (app.id=tp12-window-join). Ctrl-C pour arreter.");
    }

    /** Vrai si la ligne a au moins 3 champs et un montant numerique. */
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

    /** Devise (3e champ) en majuscules. */
    static String currencyOf(String line) {
        String[] p = line.split(";");
        return (p.length >= 3) ? p[2].toUpperCase() : "?";
    }

    /** Montant (2e champ) ; 0.0 si invalide. */
    static double amountOf(String line) {
        try {
            return Double.parseDouble(line.split(";")[1]);
        } catch (Exception e) {
            return 0.0;
        }
    }

    /** Convertit le montant via le taux ; renvoie "orderId;DEVISE;montant;base". */
    static String convert(String orderLine, String rate) {
        String[] p = orderLine.split(";");
        String orderId = p[0];
        String ccy = currencyOf(orderLine);
        double amount = amountOf(orderLine);
        double base = amount * safeParse(rate);
        double rounded = Math.round(base * 100.0) / 100.0;
        return orderId + ";" + ccy + ";" + amount + ";" + rounded;
    }

    static double safeParse(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return 0.0;
        }
    }
}
