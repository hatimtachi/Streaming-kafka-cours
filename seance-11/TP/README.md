# TP — Séance 11 : Traitement avec état (agrégations)

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Kafka Streams 4.3.0** · **Java 21** · **Maven**
> Fonctionne sous **Windows**, **macOS** et **Linux**.

Premier TP **stateful** : on quitte le pur transit pour le **calcul à mémoire**. On agrège un flux de commandes **par devise** — le **nombre** de commandes et le **chiffre d'affaires** cumulé — puis on observe les **state stores** et leurs **changelogs**, et la **tolérance aux pannes** (l'état est restauré après un redémarrage). À la fin, vous saurez `groupBy` + `count`/`aggregate`, matérialiser un agrégat, et expliquer comment le changelog reconstruit l'état.

> **Le fichier starter ne contient pas la réponse** : il décrit, étape par étape, ce que vous devez écrire. Le **corrigé complet** est dans le package `solution` (`./run.sh app solution`).

---

## 1. Le principe

```
KStream ──groupBy──▶ KGroupedStream ──count / aggregate──▶ KTable (état par clé)
```

- **`groupBy`** (nouvelle clé) ou **`groupByKey`** (clé inchangée) → un `KGroupedStream`.
- Une **agrégation** (`count`, `reduce`, `aggregate`) → un **`KTable`** : pour chaque clé, la valeur agrégée **courante**, mise à jour à chaque enregistrement.
- L'agrégat est **matérialisé** dans un **state store** local (RocksDB), sauvegardé dans un topic **changelog** compacté : `<application.id>-<store>-changelog`. C'est ce qui rend l'état **tolérant aux pannes**.

**Entrée** `orders` : lignes `orderId;amount;currency` — **Sorties** : `count-by-currency`, `total-by-currency`.

---

## 2. Prérequis

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

> Ports requis : **29092 / 29093 / 29094** (brokers) et **8080** (UI). `kafka-streams` est sur **Maven Central**.

---

## 3. Démarrage rapide

```bash
# 1. Le cluster
./up.sh                 # ou .\up.ps1   ou   docker compose up -d

# 2. Les topics (les -repartition / -changelog seront crees AUTOMATIQUEMENT par Streams)
for t in orders count-by-currency total-by-currency; do
  docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic "$t" --partitions 3 --replication-factor 3
done

# 3. Completer l'agregation puis la lancer (sinon ./run.sh app solution)
./run.sh app            # tourne en continu (Ctrl-C pour arreter)

# 4. Autre terminal : produire, puis lire les agregats par devise
./run.sh seed
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic total-by-currency --from-beginning --property print.key=true --timeout-ms 5000
```

> **Adressage :** machine (CLI/app) → `localhost:29092` ; conteneur (`kcli`, UI) → `broker-1:9092`.

---

## 4. Contenu du dossier

```
tp-seance-11/
├── docker-compose.yml         # cluster 3 brokers KRaft + Kafbat UI
├── pom.xml                    # Maven : kafka-streams (Maven Central)
├── src/main/java/fr/esgi/kafka/tp11/
│   ├── OrderProducer.java      # FOURNI : commandes de demo (devises variees)
│   ├── CurrencyAggregator.java  # >>> a completer <<< (instructions, sans le code)
│   └── solution/CurrencyAggregator.java   # le corrige complet
├── src/main/resources/simplelogger.properties
├── up.* / down.* / reset.*    # cycle de vie du cluster
├── kcli.*                     # outils CLI Kafka dans broker-1
├── run.*                      # compiler + lancer (app / seed)
└── watch-group.*              # repartition des partitions entre instances
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows. Config + helpers `currencyOf`/`amountOf` sont fournis dans le starter.)*

---

## 5. Les state stores & la tolérance aux pannes

- **State store local** : l'agrégat (`KTable`) est stocké **dans l'instance** (RocksDB par défaut, sous `state.dir` ≈ `/tmp/kafka-streams/<application.id>`). Lecture/écriture **locales**, donc rapides.
- **Changelog** : chaque mise à jour du store est aussi écrite dans un **topic Kafka interne compacté** `<application.id>-<store>-changelog`. C'est la **source de vérité durable**. `Materialized.as("nom")` fixe le nom du store (et donc du changelog) ; sans nom, Streams en génère un.
- **Restauration** : au démarrage ou après un rééquilibrage, l'instance qui reçoit une partition **rejoue son changelog** pour reconstruire le store. L'état n'est **jamais perdu**.
- **Standby replicas** (`num.standby.replicas`) : des copies tièdes du store, maintenues à jour sur d'autres instances, qui **accélèrent la bascule** en cas de panne.

> Pensez à `replication.factor = 3` (réglé dans la config) pour que les topics internes (`-repartition`, `-changelog`) soient répliqués sur les 3 brokers.

---

## 6. Atelier guidé (~45 min)

> Cluster démarré, les 3 topics créés. Le code à écrire est dans `CurrencyAggregator.java` : **les 3 étapes y sont décrites, à vous de les coder** (helpers fournis).

### Étape 1 — Grouper par devise

Sur `orders`, `groupBy((k, line) -> currencyOf(line), Grouped.with(...))` → un `KGroupedStream` (gardez-le dans une variable). `groupBy` change la clé → un topic `-repartition` apparaîtra.

### Étape 2 — Compter → `count-by-currency`

`count(Materialized.as("count-by-ccy"))` → `KTable<devise, Long>`. Un `KTable` ne s'écrit pas directement : `toStream()`, convertissez la valeur en texte (`mapValues`), puis `to(COUNT_OUT)`.

### Étape 3 — Cumuler → `total-by-currency`

`aggregate(initialiseur 0.0, agrégateur (ccy, line, sum) -> sum + amountOf(line), Materialized.with(String, Double))` → `KTable<devise, Double>` ; puis `toStream()`, `mapValues` (format), `to(TOTAL_OUT)`.

### Étape 4 — Lancer et observer

```bash
./run.sh app            # votre agregation (continu)
# autre terminal :
./run.sh seed
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic count-by-currency --from-beginning --property print.key=true --timeout-ms 5000
```

Attendu : `EUR 3`, `USD 3`, `GBP 1` (comptes) et `EUR 69.90`, `USD 155.00`, `GBP 12.00` (totaux).

### Étape 5 — Arrêter, relancer : l'état est restauré

1. **Arrêtez** l'application (Ctrl-C).
2. **Relancez** `./run.sh app`.
3. Renvoyez `./run.sh seed` : les totaux **continuent** (EUR passe à 139.80, etc.) — ils **ne repartent pas de zéro**. L'état a été **restauré depuis le changelog**.

```bash
# Voir les topics internes crees par Streams (-repartition et -changelog)
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --list
```

---

## 7. Défis

### Défi 1 — Maximum par devise avec `reduce`

Calculez le **montant maximum** par devise : `reduce((a, b) -> ...)` combine deux valeurs de **même type** (forme intermédiaire entre `count` et `aggregate`).

### Défi 2 — Exactly-once

Passez la garantie en `exactly_once_v2` (`StreamsConfig.PROCESSING_GUARANTEE_CONFIG`) : les agrégats restent **exacts** même en cas de rejeu — crucial pour des compteurs financiers.

### Défi 3 — Store en mémoire

Remplacez RocksDB par un store **in-memory** (`Materialized.as(Stores.inMemoryKeyValueStore("..."))`) et comparez : plus rapide, mais limité par la RAM — et **toujours** sauvegardé par le changelog.

> **La suite — Séance 12 :** **fenêtrage** (agréger par tranche de temps, gérer les données en retard avec une *grace period*) et **jointures** (enrichir un flux avec un autre flux ou une table : `KStream`–`KTable`, `KStream`–`KStream`).

---

## 8. Dépannage

### Kafka Streams / Maven

| Symptôme | Solution |
|---|---|
| `mvn` introuvable / `release 21 not supported` | Maven absent ou JDK < 21 (voir § 2). |
| Une sortie est vide | Les topics existent-ils ? L'app tourne-t-elle ? `./run.sh seed` lancé ? Consumer avec `--from-beginning` ? |
| La sortie s'affiche en octets illisibles | Lisez les sorties **textuelles** (`count-by-currency` / `total-by-currency`) : la solution convertit Long/Double en texte avant `to(...)`. |
| `ClassCastException` (Long/Double) | Le `Materialized` de l'`aggregate` doit fixer la valeur en `Double` ; `count` produit du `Long`. |
| Devises non regroupées (`eur` et `EUR` séparés) | `currencyOf` normalise en majuscules — utilisez-le dans `groupBy`. |
| Au redémarrage, les totaux repartent de **zéro** | Vous avez effacé l'état : `reset` supprime aussi `/tmp/kafka-streams/...`. Pour **démontrer la reprise**, NE faites PAS de reset entre deux lancements. |
| `InconsistentGroupProtocolException` | Ancien état local incompatible : `rm -rf /tmp/kafka-streams/tp11-currency-aggregator` puis relancez. |

### Cluster / Docker / Windows

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (29092/8080) | Un autre kit tourne (S1–S10). `docker compose ls` puis `docker compose down`. |
| `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous `(healthy)`. |
| `... .ps1 cannot be loaded ...` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 app`. |
| Sous **Git Bash**, chemins malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1`. |

> **macOS Apple Silicon** : images Kafka multi-arch (natif). **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix run.sh`.

---

## 9. Commandes utiles (mémo)

```
# Cluster + topics (Linux/macOS)
./up.sh | .\up.ps1 | docker compose up -d
for t in orders count-by-currency total-by-currency; do \
  docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic "$t" --partitions 3 --replication-factor 3; done
# Windows PowerShell :
foreach ($t in "orders","count-by-currency","total-by-currency") { \
  docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic $t --partitions 3 --replication-factor 3 }

# Agregation + production + lecture
./run.sh app                 | .\run.ps1 app                 # votre agregation (continu)
./run.sh app solution        | .\run.ps1 app solution        # corrige
./run.sh seed                | .\run.ps1 seed
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic total-by-currency --from-beginning --property print.key=true --timeout-ms 5000

# Topics internes (-repartition, -changelog)
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --list
```

---

## Annexe — Cluster identique aux séances précédentes

L'infrastructure (3 brokers KRaft, `localhost:29092` côté hôte, `broker-N:9092` en interne, Kafbat UI) est la même qu'aux séances précédentes ; seul le nom de projet change (`tp-kafka-s11`). Cette séance reste en `String` (ni Schema Registry ni Connect). Les topics internes de Streams sont répliqués grâce à `replication.factor = 3`. Pour l'adressage hôte/conteneur, voir le README de la S5.
