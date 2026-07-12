package fr.esgi.kafka.tp5;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TP S5 - Producteur a completer (etape 1 de l'atelier).
 *
 * Objectif : envoyer 30 messages dans le topic "events", avec une CLE = userId
 *            (par ex. u1..u5), pour que les evenements d'un meme utilisateur
 *            tombent toujours dans la meme partition (ordre par utilisateur).
 *
 * Lancer   : ./run.sh producer        (ou .\run.ps1 producer)
 * Solution : package fr.esgi.kafka.tp5.solution
 */
public class EventProducer {

    public static void main(String[] args) {
        var props = new Properties();

        // TODO 1 - Configurer le producteur :
        //   ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      = "localhost:29092"
        //   ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   = StringSerializer.class.getName()
        //   ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG = StringSerializer.class.getName()
        //   ProducerConfig.ACKS_CONFIG                   = "all"

        // TODO 2 - Dans un try-with-resources, creer un KafkaProducer<String,String>
        //          et envoyer 30 messages :
        //            - cle    = un userId parmi quelques-uns, ex. "u" + (i % 5 + 1)
        //            - valeur = "event-" + i
        //          Afficher la partition et l'offset via un callback.

        System.out.println("EventProducer a completer : voir les TODO.");
    }
}
