package fr.esgi.kafka.tp5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * TP S5 - Consommateur de groupe (starter).
 *
 * Il FONCTIONNE deja, en commit AUTOMATIQUE. Lancez-le dans plusieurs terminaux
 * (meme group.id "atelier-s5") pour observer le rebalance -- etapes 2 a 4 :
 *
 *   ./run.sh consumer            (ou .\run.ps1 consumer)
 *
 * Arret propre : Ctrl-C (envoie LeaveGroup -> rebalance immediat).
 *
 * ETAPE 5 - passer en COMMIT MANUEL (at-least-once) : suivre les deux TODO.
 * Solution complete : package fr.esgi.kafka.tp5.solution
 */
public class GroupConsumer {

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "atelier-s5");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // TODO 5a - Desactiver le commit automatique :
        //   props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        var consumer = new KafkaConsumer<String, String>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of("events"));
            System.out.println("GroupConsumer demarre (group.id=atelier-s5). Ctrl-C pour arreter.");
            while (true) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("p=%d off=%d key=%s %s%n",
                            r.partition(), r.offset(), r.key(), r.value());
                }
                // TODO 5b - Apres avoir traite TOUT le lot, committer manuellement :
                //   consumer.commitSync();
            }
        } catch (WakeupException e) {
            // arret demande
        } finally {
            consumer.close();   // envoie LeaveGroup -> rebalance immediat
            System.out.println("GroupConsumer arrete.");
        }
    }
}
