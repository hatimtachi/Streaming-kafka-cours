package fr.esgi.kafka.tp14;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests de la topologie TP14 avec TopologyTestDriver : AUCUN cluster requis.
 * On pipe des entrees, on lit la sortie et on inspecte le state store, en synchrone.
 *
 *   mvn -q test
 */
class CountTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> in;
    private TestOutputTopic<String, String> out;
    private KeyValueStore<String, Long> store;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Desactiver le cache : chaque mise a jour est emise -> sorties deterministes
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(CountApiApp.buildTopology(), props);
        in = driver.createInputTopic("orders",
                new StringSerializer(), new StringSerializer());
        out = driver.createOutputTopic("count-by-currency",
                new StringDeserializer(), new StringDeserializer());
        store = driver.getKeyValueStore(CountApiApp.STORE);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void compte_par_devise() {
        in.pipeInput("k", "o-1;19.9;eur");
        in.pipeInput("k", "o-2;5;eur");
        in.pipeInput("k", "o-3;9;usd");
        assertEquals(Long.valueOf(2), store.get("EUR"));
        assertEquals(Long.valueOf(1), store.get("USD"));
    }

    @Test
    void ignore_les_lignes_invalides() {
        in.pipeInput("k", "ligne-cassee");
        assertNull(store.get("EUR"));
        assertTrue(out.isEmpty());
    }

    @Test
    void emet_dans_le_topic_de_sortie() {
        in.pipeInput("k", "o-1;10;gbp", Instant.ofEpochMilli(1000));
        assertFalse(out.isEmpty());
        KeyValue<String, String> kv = out.readKeyValue();
        assertEquals("GBP", kv.key);
        assertEquals("1", kv.value);
    }
}
