# TP — Séance 9 : Kafka Streams (première topologie)

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Kafka Streams 4.3.0** · **Java 21** · **Maven**
> Fonctionne sous **Windows**, **macOS** et **Linux**.

Premier TP de l'arc **Kafka Streams**. On écrit une **topologie** *stateless* : lire `orders`, **filtrer** par montant, **normaliser** la devise, et réécrire dans `orders-filtered`. À la fin, vous saurez décrire une topologie avec le **DSL** (`StreamsBuilder`, `KStream`), la configurer et la lancer, distinguer **KStream** (faits) et **KTable** (états), et observer le **parallélisme** en lançant plusieurs instances.

> **Kafka Streams est une bibliothèque** (un jar dans votre application), **pas un cluster** à déployer. Le code tourne **sur votre machine** et se connecte en **`localhost:29092`**. On passe à l'échelle en lançant **plusieurs instances** avec le **même `application.id`** (elles forment un consumer group). Cette séance reste **stateless** ; l'état (agrégations, jointures, fenêtres) arrive en S11.

---

## 1. Prérequis

| Système | Docker | JDK 21 + Maven | Terminal | Scripts |
|---|---|---|---|---|
| **Windows 10/11** | Docker Desktop (WSL 2) | Adoptium Temurin 21 + Maven | **PowerShell** | `.ps1` |
| **macOS** | Docker Desktop | `brew install openjdk@21 maven` | Terminal | `.sh` |
| **Linux** | Docker Engine + Compose v2 | `openjdk-21-jdk` + `maven`, ou **SDKMAN** | votre shell | `.sh` |

```
docker compose version
java -version      # 21
mvn -version
```

> Ports requis : **29092 / 29093 / 29094** (brokers) et **8080** (UI).
> `kafka-streams` est sur **Maven Central** (pas de dépôt Confluent ici — on reste en `String`).

---

## 2. Démarrage rapide

```bash
# 1. Le cluster
./up.sh                 # ou .\up.ps1   ou   docker compose up -d
docker compose ps       # 3 brokers (healthy)

# 2. Les deux topics (3 partitions pour observer le parallelisme)
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders          --partitions 3 --replication-factor 3
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders-filtered --partitions 3 --replication-factor 3

# 3. Lancer la topologie (apres les TODO ; sinon ./run.sh app solution)
./run.sh app            # tourne en continu (Ctrl-C pour arreter)

# 4. Dans un AUTRE terminal : produire des commandes, puis lire la sortie
./run.sh seed
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-filtered --from-beginning --timeout-ms 5000
```

> **Adressage :** machine (CLI/app Streams) → **`localhost:29092`** ; conteneur (`kcli`, UI) → **`broker-1:9092`**.

---

## 3. Contenu du dossier

```
tp-seance-9/
├── docker-compose.yml         # cluster 3 brokers KRaft + Kafbat UI
├── pom.xml                    # Maven : kafka-streams (Maven Central)
├── src/main/java/fr/esgi/kafka/tp9/
│   ├── OrderProducer.java      # FOURNI : envoie des commandes de demo dans "orders"
│   ├── OrderStreamApp.java      # >>> a completer <<< (config + topologie)
│   └── solution/OrderStreamApp.java
├── src/main/resources/simplelogger.properties
├── up.* / down.* / reset.*    # cycle de vie du cluster
├── kcli.*                     # outils CLI Kafka dans broker-1
├── run.*                      # compiler + lancer (app / seed)
└── watch-group.*              # voir la repartition des partitions entre instances
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows.)*

---

## 4. Les idées clés

- **Une bibliothèque, pas un cluster.** Kafka Streams s'embarque dans votre application (elle utilise le consumer/producer en interne). Pour scaler, on lance **N instances** avec le même `application.id` → un **consumer group**, les partitions se répartissent et se rééquilibrent en cas de panne. Le **maximum utile** d'instances = le nombre de partitions du topic d'entrée.
- **Une topologie** = un **graphe** orienté : des *sources* (topics), des *processeurs* (les opérations), des *sinks* (topics). On le décrit avec le **DSL** (`StreamsBuilder` + `KStream`/`KTable`) ; la *Processor API* offre un contrôle bas niveau. `builder.build()` produit la `Topology`, qu'on remet à un `KafkaStreams` pour l'exécuter — on **décrit le quoi**, le runtime gère le comment.
- **Style fonctionnel** : chaque opérateur renvoie un **nouveau** `KStream` immuable — on enchaîne, on ne mute pas. `mapValues` (plutôt que `map`) **préserve la clé** → évite une repartition.
- **`application.id`** est central : identifiant du **consumer group** *et* préfixe des topics internes / du **répertoire d'état local**. Deux applications différentes **doivent** avoir des `application.id` différents.
- **KStream vs KTable** : un `KStream` est un flux de **faits** (chaque message s'ajoute) ; un `KTable` est une table d'**états** (pour chaque clé, la **dernière** valeur — *upsert* ; `null` = suppression). C'est le pendant des topics **compactés** (S3). **Dualité** : agréger un flux donne une table (`toTable`) ; relire une table donne un flux (`toStream`).

> **Note Kafka 4.x** : `transform`/`transformValues` ont été retirés (→ `process`/`processValues`), et l'ancien `branch()` a disparu (→ `split()`/`Branched`). Le DSL stateless de ce TP (`filter`, `mapValues`, `to`) est inchangé.

---

## 5. Atelier guidé (~45 min)

> Cluster démarré, topics `orders` / `orders-filtered` créés.
> Code à compléter : `src/main/java/fr/esgi/kafka/tp9/OrderStreamApp.java` (helpers `amountOf`/`upperCurrency` fournis).

### Étape 1 — Configurer l'application

TODO 1 : `APPLICATION_ID_CONFIG = "tp9-order-stream"`, `BOOTSTRAP_SERVERS_CONFIG = "localhost:29092"`, et les serdes par défaut (`DEFAULT_KEY_SERDE_CLASS_CONFIG` / `DEFAULT_VALUE_SERDE_CLASS_CONFIG` = `Serdes.String().getClass()`).

### Étape 2 — Compléter la topologie

TODO 2 : sur le `KStream orders`, enchaînez `filter((key, line) -> amountOf(line) >= 10.0)` puis `.mapValues(OrderStreamApp::upperCurrency)`.

### Étape 3 — Écrire le résultat et lancer

TODO 3 : terminez par `.to(OUT, Produced.with(Serdes.String(), Serdes.String()))`, puis lancez :

```bash
./run.sh app            # l'application tourne et attend des messages
```

### Étape 4 — Produire et vérifier

Dans un **autre terminal** :

```bash
./run.sh seed           # 6 commandes ; 4 doivent passer le filtre
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-filtered --from-beginning --timeout-ms 5000
```

Une commande `< 10` (ex. `o-2;5.0;usd`) **n'apparaît pas** ; la devise ressort en **majuscules** (`eur` → `EUR`).

### Étape 5 — Le parallélisme

Lancez une **deuxième instance** (même `application.id`) dans un troisième terminal, et observez le rééquilibrage :

```bash
./run.sh app            # 2e instance
./watch-group.sh        # les 3 partitions se repartissent entre les 2 instances (ex. 2+1)
```

Arrêtez une instance : ses partitions repassent à l'autre. C'est le mécanisme des **consumer groups** (S5), réutilisé par Streams pour l'élasticité et la tolérance aux pannes.

---

## 6. Défis

### Défi 1 — Brancher avec `split()`

Routez les commandes vers **deux topics** selon la devise (ex. `orders-eur` / `orders-other`) avec `split()` + `Branched` (l'ancien `branch()` a disparu en 4.0).

### Défi 2 — Recléer et observer la repartition

Faites un `selectKey((k, line) -> currencyOf(line))` : changer la clé oblige Streams à **repartitionner** pour regrouper par nouvelle clé → un **topic de repartition interne** apparaît (`...-repartition`). Constatez-le avec `kafka-topics --list`.

### Défi 3 — Avro (pont avec la S7)

Remplacez les `Serdes.String()` par un **`SpecificAvroSerde`** (Confluent) configuré avec `schema.registry.url` vers un Schema Registry (comme en S7). Cela nécessite le dépôt Confluent + un registre démarré (voir le kit S7).

> **La suite — Séance 10 :** un TP pour approfondir le **stateless** (router, filtrer et enrichir plusieurs flux dans une même topologie), avant d'attaquer le **stateful** en S11.

---

## 7. Dépannage

### Kafka Streams / Maven

| Symptôme | Solution |
|---|---|
| `mvn` introuvable / `release 21 not supported` | Maven absent ou JDK < 21 (voir § 1). |
| `TopologyException` / l'app démarre puis s'arrête | La topologie est incomplète (TODO non faits) ou un serde manque. Comparez avec `./run.sh app solution`. |
| Rien dans `orders-filtered` | L'app tourne-t-elle ? Les topics existent-ils ? Avez-vous lancé `./run.sh seed` ? Le consumer a-t-il `--from-beginning` ? |
| Tout est filtré | `amountOf` ne parse pas (séparateur `;`, format `orderId;amount;currency`). |
| `InconsistentGroupProtocolException` au lancement de la 2e instance | Vous mélangez deux `application.id` ou un ancien état local traîne : `rm -rf /tmp/kafka-streams/tp9-order-stream` puis relancez. |
| Au redémarrage, comportement étrange | Streams garde un **état local** (`/tmp/kafka-streams/<application.id>`). Pour repartir propre, supprimez ce dossier (voir `reset`). |

### Cluster / Docker

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (29092/8080) | Un autre kit tourne (S1–S8). `docker compose ls` puis `docker compose down`. |
| `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous `(healthy)`. |

### Windows

| Symptôme | Solution |
|---|---|
| `... .ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 app`. |
| Lancer 2 instances | Ouvrez deux fenêtres PowerShell et faites `.\run.ps1 app` dans chacune. |
| Sous **Git Bash**, `/opt/...` ou `broker-1:9092` malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1`. |

> **macOS Apple Silicon** : images Kafka multi-arch (natif). **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix run.sh`.

---

## 8. Commandes utiles (mémo)

```
# Cluster + topics
./up.sh | .\up.ps1 | docker compose up -d        docker compose ps
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders --partitions 3 --replication-factor 3
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders-filtered --partitions 3 --replication-factor 3

# Application Streams + production + verification
./run.sh app                 | .\run.ps1 app                 # votre topologie (continu)
./run.sh app solution        | .\run.ps1 app solution        # corrige
./run.sh seed                | .\run.ps1 seed                # produire des commandes
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-filtered --from-beginning --timeout-ms 5000

# Parallelisme (consumer group = application.id)
./watch-group.sh             | .\watch-group.ps1
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --list
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group tp9-order-stream
```

---

## Annexe — Cluster identique aux séances précédentes

L'infrastructure (3 brokers KRaft, `localhost:29092` côté hôte, `broker-N:9092` en interne, Kafbat UI) est la même qu'aux séances précédentes ; seul le nom de projet change (`tp-kafka-s9`). Cette séance n'utilise **ni** Schema Registry **ni** Connect (on reste en `String`) ; le défi Avro réutiliserait le registre de la S7. Pour le détail de l'adressage hôte/conteneur, voir le README de la S5.
