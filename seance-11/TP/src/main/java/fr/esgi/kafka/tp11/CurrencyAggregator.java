package fr.esgi.kafka.tp11;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Locale;
import java.util.Properties;

/**
 * TP S11 - Agregation par devise (A COMPLETER).
 *
 * Entree  : topic "orders", lignes  orderId;amount;currency
 * Sorties : count-by-currency (nb de commandes / devise)
 *           total-by-currency (chiffre d'affaires cumule / devise)
 *
 * Ce fichier ne contient PAS la solution : il decrit, etape par etape, CE QUE
 * vous devez construire. A vous d'ecrire le code de l'agregation (le corrige
 * complet est dans le package solution : ./run.sh app solution).
 *
 * La configuration et les helpers (currencyOf / amountOf) sont fournis.
 */
public class CurrencyAggregator {

    static final String IN        = "orders";
    static final String COUNT_OUT = "count-by-currency";
    static final String TOTAL_OUT = "total-by-currency";

    public static void main(String[] args) {
        // --- Configuration (fournie) ---
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tp11-currency-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Topics internes (-repartition, -changelog) repliques sur les 3 brokers :
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        var builder = new StreamsBuilder();
        KStream<String, String> orders =
            builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()));

        // ============== A COMPLETER : l'agregation par devise (3 etapes) ==============
        //
        // Schema mental :  KStream --(groupBy)--> KGroupedStream --(count/aggregate)--> KTable
        //
        // ETAPE 1 - Grouper par devise
        //   * A partir de "orders", construisez un KGroupedStream regroupe par DEVISE :
        //     groupBy avec une fonction (cle, valeur) -> currencyOf(valeur),
        //     et precisez les Serdes du flux groupe via Grouped.with(String, String).
        //   * Conservez ce KGroupedStream dans une variable (ex. "byCcy") : il sert aux 2 agregations.
        //   * Note : groupBy CHANGE la cle -> Streams creera un topic interne "-repartition".
        //
        // ETAPE 2 - Compter les commandes par devise -> COUNT_OUT
        //   * Sur byCcy, appliquez count(...) en NOMMANT le store : Materialized.as("count-by-ccy").
        //     count renvoie un KTable<devise, Long>.
        //   * Un KTable n'ecrit pas directement dans un topic : convertissez-le en flux avec
        //     toStream(), transformez la valeur Long en texte lisible (mapValues), puis to(COUNT_OUT).
        //
        // ETAPE 3 - Cumuler le chiffre d'affaires par devise -> TOTAL_OUT
        //   * Sur byCcy, appliquez aggregate(...) avec :
        //       - un initialiseur : la valeur de depart, 0.0 ;
        //       - un agregateur (devise, valeur, accumulateur) -> accumulateur + amountOf(valeur) ;
        //       - un Materialized fixant les Serdes du RESULTAT : cle String, valeur Double
        //         (Materialized.with(Serdes.String(), Serdes.Double())).
        //     aggregate renvoie un KTable<devise, Double>.
        //   * Comme au comptage : toStream(), formatez le Double en texte (mapValues), puis to(TOTAL_OUT).
        //     (Astuce format : String.format(Locale.ROOT, "%.2f", valeur).)
        //
        // (Helpers fournis : currencyOf(ligne) -> devise en MAJUSCULES ; amountOf(ligne) -> montant.)
        // ==============================================================================

        var streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        System.out.println("(agregation a completer) " + IN + " -> {" + COUNT_OUT + ", " + TOTAL_OUT + "}");
        streams.start();
    }

    // ---- Helpers fournis (a utiliser, ne pas modifier) ----

    /** Devise d'une ligne "orderId;amount;currency", normalisee en MAJUSCULES. */
    static String currencyOf(String line) {
        var p = line.split(";");
        return (p.length >= 3) ? p[2].trim().toUpperCase(Locale.ROOT) : "?";
    }

    /** Montant d'une ligne (0 si illisible). */
    static double amountOf(String line) {
        try {
            return Double.parseDouble(line.split(";")[1]);
        } catch (Exception e) {
            return 0.0;
        }
    }
}
