package fr.esgi.kafka.tp10;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * TP S10 - Topologie de routage stateless (A COMPLETER).
 *
 * Entree  : topic "orders", lignes  orderId;amount;currency;sku1|sku2|...
 * Sorties : orders-priority / orders-standard (par montant),
 *           order-items (un article par ligne), orders-rejected (lignes malformees).
 *
 * Ce fichier ne contient PAS la solution : il decrit, etape par etape, CE QUE
 * vous devez construire. A vous d'ecrire le code de la topologie (le corrige
 * complet est dans le package solution : ./run.sh app solution).
 *
 * La configuration et les helpers (isValid / amountOf / explodeItems) sont fournis.
 */
public class OrderRouterApp {

    static final String IN       = "orders";
    static final String REJECTED = "orders-rejected";
    static final String PRIORITY = "orders-priority";
    static final String STANDARD = "orders-standard";
    static final String ITEMS    = "order-items";

    public static void main(String[] args) {
        // --- Configuration (fournie, identique a la S9) ---
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tp10-order-router");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        var builder = new StreamsBuilder();
        KStream<String, String> orders =
            builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()));

        // ================= A COMPLETER : la topologie (3 etapes) =================
        //
        // ETAPE 1 - Valider et mettre a l'ecart (dead-letter)
        //   a) Envoyez les lignes INVALIDES vers le topic REJECTED.
        //      - Filtrez le flux "orders" avec filterNot et le helper isValid.
        //      - filterNot attend un predicat (cle, valeur) -> booleen : ignorez la
        //        cle et testez la valeur, par ex. (k, v) -> isValid(v).
        //      - Terminez par .to(REJECTED).
        //   b) Construisez un KStream nomme "valid" qui ne garde QUE les lignes
        //      valides (meme principe avec filter). Il servira aux etapes 2 et 3.
        //
        // ETAPE 2 - Router par palier de montant avec split()
        //   - Sur "valid", ouvrez un split() (vous pouvez le nommer : Named.as("tier-")).
        //   - Premiere branche : si le montant (amountOf) >= 100.0, ecrire dans PRIORITY.
        //       L'action d'une branche se decrit avec Branched.withConsumer(...),
        //       qui recoit le sous-flux : ks -> ks.to(PRIORITY).
        //   - Branche par defaut (defaultBranch) : tout le reste -> STANDARD.
        //   - Rappels : les predicats sont evalues DANS L'ORDRE (le premier vrai capture
        //     l'enregistrement) ; SANS defaultBranch, les non-apparies seraient perdus.
        //
        // ETAPE 3 - Eclater une commande en articles
        //   - Sur "valid", appliquez flatMapValues avec le helper explodeItems
        //     (il renvoie une liste de chaines "orderId;sku", une par article ;
        //      la cle d'origine est conservee), puis ecrivez le resultat dans ITEMS.
        //
        // (Aide : tous les .to(...) ici utilisent les serdes par defaut, donc to("nom") suffit.)
        // =========================================================================

        var streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        System.out.println("(topologie a completer) " + IN + " -> {priority, standard, items, rejected}");
        streams.start();
    }

    // ---- Helpers fournis (a utiliser, ne pas modifier) ----

    /** Vrai si la ligne est bien formee : 4 champs et un montant numerique. */
    static boolean isValid(String line) {
        var p = line.split(";");
        if (p.length != 4) return false;
        if (p[0].isBlank() || p[3].isBlank()) return false;
        try {
            Double.parseDouble(p[1]);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Montant d'une ligne (0 si illisible). */
    static double amountOf(String line) {
        try {
            return Double.parseDouble(line.split(";")[1]);
        } catch (Exception e) {
            return 0.0;
        }
    }

    /** Eclate une commande en une ligne "orderId;sku" par article (separateur '|'). */
    static List<String> explodeItems(String line) {
        var out = new ArrayList<String>();
        var p = line.split(";");
        if (p.length < 4) return out;
        var orderId = p[0];
        for (var sku : p[3].split("\\|")) {
            if (!sku.isBlank()) out.add(orderId + ";" + sku);
        }
        return out;
    }
}
