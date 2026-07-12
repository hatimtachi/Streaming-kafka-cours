# TP — Séance 6 : Exactly-once (idempotence & transactions)

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Java 21** · **Maven**
> Fonctionne sous **Windows**, **macOS** et **Linux**.

On construit un **pipeline exactly-once** *consume-transform-produce* : lire `orders`, valider, et reproduire vers `orders-validated` **de façon atomique** (la sortie **et** les offsets dans une même transaction). À la fin, vous saurez configurer un **producteur transactionnel**, gérer un **message empoisonné** par `abortTransaction()`, et vérifier l'exactly-once en lisant en **`read_committed`**.

> Rappel : l'exactly-once de Kafka vaut **à l'intérieur d'un cluster Kafka** — pas vers un système externe (base, API). Le code Java tourne **sur votre machine** et se connecte en **`localhost:29092`**.

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

> Ports requis : **29092 / 29093 / 29094** (brokers) et **8080** (UI). Ce TP suppose les acquis de la **S5** (commit des offsets).

---

## 2. Démarrage rapide

```bash
# 1. Le cluster 3 brokers (le coordinateur de transactions a besoin de __transaction_state en RF 3)
./up.sh                 # ou .\up.ps1   ou   docker compose up -d
docker compose ps       # 3 brokers (healthy)

# 2. Les deux topics du pipeline
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders           --partitions 3 --replication-factor 3
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders-validated --partitions 3 --replication-factor 3

# 3. Alimenter "orders" (seeder fourni : 10 commandes dont 1 empoisonnée)
./run.sh seed           # .\run.ps1 seed

# 4. Compléter puis lancer le pipeline, et lire la sortie validée
./run.sh pipeline                # (après les TODO ; sinon ./run.sh pipeline solution)
./read-validated.sh              # lit orders-validated en read_committed
```

> **Adressage :** machine (CLI/Java) → **`localhost:29092`** ; conteneur (UI, `kcli`) → **`broker-1:9092`**.

---

## 3. Contenu du dossier

```
tp-seance-6/
├── docker-compose.yml         # cluster 3 brokers (RF 3, __transaction_state répliqué)
├── pom.xml                    # projet Maven : kafka-clients
├── src/main/java/fr/esgi/kafka/tp6/
│   ├── OrderSeeder.java        # FOURNI : alimente "orders" (1 message empoisonné)
│   ├── EosPipeline.java        # >>> à compléter <<< (consume-transform-produce)
│   └── solution/EosPipeline.java
├── src/main/resources/simplelogger.properties
├── up.* / down.* / reset.*    # cycle de vie du cluster
├── kcli.*                     # outils CLI Kafka dans broker-1
├── run.*                      # compiler + lancer (seed / pipeline)
└── read-validated.*           # lire orders-validated (read_committed / read_uncommitted)
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows.)*

---

## 4. Les idées clés

- **Idempotence du producteur** (PID + numéro de séquence) : supprime les doublons de *retry* **sur une partition**. **Activée par défaut depuis Kafka 3.0** (`acks=all`, `enable.idempotence=true`). Un `console-producer` simple est déjà idempotent en 4.x.
- **Transactions** : un `transactional.id` stable identifie le producteur (et **implique l'idempotence**). Écriture **atomique** sur plusieurs partitions/topics **et** sur les offsets. Cycle : `initTransactions()` (une fois) → puis, par lot, `beginTransaction` → `send` → `sendOffsetsToTransaction` → `commit` / `abort`.
- **`read_committed`** : le consommateur ne voit que les messages **validés** (les transactions abandonnées sont filtrées).
- **consume-transform-produce** = le pattern exactly-once **à l'intérieur** de Kafka.

> Ce kit traite **une transaction par message** : seul le message empoisonné est écarté (la sortie compte exactement les commandes non empoisonnées). Committer **par lot** est plus rapide — c'est l'objet du défi.

---

## 5. Atelier guidé (~45 min)

> Cluster démarré, topics `orders` / `orders-validated` créés, `orders` alimenté (`./run.sh seed`).
> Le code à compléter est dans `src/main/java/fr/esgi/kafka/tp6/EosPipeline.java` (helpers fournis en bas du fichier).

### Étape 1 — Configurer le consommateur

Ajoutez (TODO 1) : `ENABLE_AUTO_COMMIT_CONFIG = "false"` (les offsets seront commités **par la transaction**) et `ISOLATION_LEVEL_CONFIG = "read_committed"`.

### Étape 2 — Configurer le producteur transactionnel

Ajoutez (TODO 2a) `TRANSACTIONAL_ID_CONFIG = "eos-pipeline-tx-1"`, puis (TODO 2b) appelez **une fois** `producer.initTransactions()` avant la boucle.

### Étape 3 — La boucle transactionnelle

Pour chaque message (TODO 3) : `beginTransaction()` → `send(toValidated(r))` → `sendOffsetsToTransaction(nextOffset(r), consumer.groupMetadata())` → `commitTransaction()`. La sortie **et** l'offset deviennent visibles **ensemble**.

### Étape 4 — Gérer le message empoisonné

`toValidated(r)` lève `PoisonException` sur le message `POISON` (TODO 4) : dans le `catch`, appelez **`abortTransaction()`** (rien n'est produit), puis avancez l'offset au-delà (petit `commit` sans sortie) pour ne pas boucler.

### Étape 5 — Lancer et vérifier

```bash
./run.sh pipeline                       # votre code (Ctrl-C pour arrêter)
./read-validated.sh                     # read_committed : 9 messages "validated:..."
./read-validated.sh read_uncommitted    # le poison n'est de toute façon jamais produit ici
```

Vérifiez l'**exactly-once** : relancez le pipeline → le décompte en `read_committed` **ne bouge pas** (pas de doublon, car les offsets ont été commités dans la transaction).

```bash
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group eos-pipeline
```

Les offsets du groupe n'avancent **que** lorsqu'une transaction est validée.

---

## 6. Défis

### Défi 1 — Crash test (exactly-once de bout en bout)

Pendant que le pipeline traite, **tuez-le brutalement** (fermez le terminal, ou `kill`). Relancez `./run.sh pipeline`, puis `./read-validated.sh` : **aucun demi-résultat**, **aucun doublon** — chaque transaction était soit entièrement validée, soit entièrement abandonnée.

### Défi 2 — Le coût des transactions

Comparez le débit **avec** et **sans** transactions, et **commit par message** vs **par lot** : en regroupant `send` + un seul `commit` par lot (au lieu d'une transaction par message), le débit monte mais la sortie n'est visible qu'au commit (latence). Les transactions coûtent typiquement quelques ms de latence et 10–20 % de débit → on réserve l'EOS aux flux critiques.

### Défi 3 — Voir le PID & la séquence dans le log

L'idempotence repose sur un `producerId` (PID) et un numéro de séquence par partition. Inspectez un segment :

```bash
docker compose exec broker-1 /opt/kafka/bin/kafka-dump-log.sh --files /var/lib/kafka/data/orders-0/00000000000000000000.log --print-data-log
```

Cherchez `producerId`, `producerEpoch`, `baseSequence` dans les *recordBatch* — exactement ce que le broker utilise pour dédoublonner. *(Le numéro de partition `orders-0` peut varier ; adaptez à une partition qui contient des données.)*

> **La suite — Séance 7 :** sérialisation & **Schema Registry** (Avro, Protobuf), et évolution compatible des formats de messages.

---

## 7. Dépannage

### Java / Maven / pipeline

| Symptôme | Solution |
|---|---|
| `mvn` introuvable / `release 21 not supported` | Maven absent ou JDK < 21 (voir § 1). |
| `TransactionalIdAuthorizationException` / blocage à l'init | Le `transactional.id` doit être défini et le cluster sain. `docker compose ps` → 3 brokers `(healthy)`. |
| Le pipeline semble figé au démarrage | `initTransactions()` contacte le coordinateur : si un broker manque, attendez que les 3 soient `(healthy)`. |
| `read-validated` ne montre rien | Avez-vous lancé le pipeline ? Les messages n'apparaissent qu'**après** `commitTransaction()`. |
| Le poison apparaît en sortie | Vérifiez que `toValidated` lève bien sur `POISON` et que le `catch` fait `abortTransaction()` (pas `commit`). |
| Doublons après relance | Vérifiez `sendOffsetsToTransaction(...)` **dans** la transaction (sinon les offsets ne sont pas atomiques). |

### Cluster / Docker

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (29092/8080) | Un autre cluster tourne (S1–S5). `docker compose ls` puis `docker compose down`. |
| `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous `(healthy)`. |
| `kafka-dump-log` : fichier introuvable | Le chemin/segment dépend de la partition. Listez `docker compose exec broker-1 ls /var/lib/kafka/data` pour trouver `orders-N`. |

### Windows

| Symptôme | Solution |
|---|---|
| `... .ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 pipeline`. |
| Sous **Git Bash**, `/opt/...` ou `broker-1:9092` malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1` (et `winpty` pour l'interactif). |

> **macOS Apple Silicon** : images Kafka multi-arch (natif). **Linux** : `permission denied docker.sock` → `sudo usermod -aG docker $USER` puis reconnexion.
> **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix run.sh`.

---

## 8. Commandes utiles (mémo)

```
# Cluster + topics
./up.sh | .\up.ps1 | docker compose up -d        docker compose ps
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders --partitions 3 --replication-factor 3
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders-validated --partitions 3 --replication-factor 3

# Pipeline + vérification
./run.sh seed                | .\run.ps1 seed
./run.sh pipeline            | .\run.ps1 pipeline             # votre code
./run.sh pipeline solution   | .\run.ps1 pipeline solution    # corrigé
./read-validated.sh                   | .\read-validated.ps1                    # read_committed
./read-validated.sh read_uncommitted  | .\read-validated.ps1 read_uncommitted
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group eos-pipeline
```

---

## Annexe — Cluster identique à la S5

L'infrastructure (3 brokers KRaft, `localhost:29092` côté hôte, `broker-N:9092` en interne, Kafbat UI) est **la même qu'en S5** ; seul le nom de projet change (`tp-kafka-s6`) et `__transaction_state` est répliqué (RF 3, min ISR 2) pour un coordinateur de transactions fiable. Pour le détail de l'adressage, voir le README de la S5.
