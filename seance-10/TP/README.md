# TP — Séance 10 : Flux stateless (router, éclater, filtrer)

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Kafka Streams 4.3.0** · **Java 21** · **Maven**
> Fonctionne sous **Windows**, **macOS** et **Linux**.

TP qui prolonge la S9 : on assemble une **topologie ramifiée** (toujours **stateless**). Un flux de commandes est **validé** (les lignes malformées partent en *dead-letter*), **routé** par montant avec `split()`, et **éclaté** en articles avec `flatMapValues`. À la fin, vous saurez composer une topologie à plusieurs branches, distinguer les opérateurs qui préservent la clé de ceux qui coûtent une **repartition**, et observer le parallélisme.

> **Le fichier starter ne contient pas la réponse** : il décrit, étape par étape, ce que vous devez écrire. Le **corrigé complet** est dans le package `solution` (`./run.sh app solution`).

---

## 1. Le scénario

```
orders ─▶ valider ─▶ router & éclater ─▶ 4 sorties
```

**Entrée** `orders` : lignes `orderId;amount;currency;sku1|sku2|...`
**Sorties :**

| Topic | Contenu |
|---|---|
| `orders-priority` | commandes de montant **≥ 100** |
| `orders-standard` | les autres commandes valides |
| `order-items` | **un article par ligne** (`orderId;sku`) |
| `orders-rejected` | lignes **malformées** (dead-letter) |

C'est un mini-ETL de streaming entièrement stateless.

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

# 2. Les CINQ topics (3 partitions chacun)
for t in orders orders-rejected orders-priority orders-standard order-items; do
  docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic "$t" --partitions 3 --replication-factor 3
done
# (Windows PowerShell : voir le memo en bas)

# 3. Completer la topologie puis la lancer (sinon ./run.sh app solution)
./run.sh app            # tourne en continu (Ctrl-C pour arreter)

# 4. Dans un AUTRE terminal : produire, puis verifier chaque sortie
./run.sh seed
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-priority --from-beginning --timeout-ms 5000
```

> **Adressage :** machine (CLI/app) → `localhost:29092` ; conteneur (`kcli`, UI) → `broker-1:9092`.

---

## 4. Contenu du dossier

```
tp-seance-10/
├── docker-compose.yml         # cluster 3 brokers KRaft + Kafbat UI
├── pom.xml                    # Maven : kafka-streams (Maven Central)
├── src/main/java/fr/esgi/kafka/tp10/
│   ├── OrderProducer.java      # FOURNI : commandes de demo (dont 2 lignes malformees)
│   ├── OrderRouterApp.java      # >>> a completer <<< (instructions, sans le code)
│   └── solution/OrderRouterApp.java   # le corrige complet
├── src/main/resources/simplelogger.properties
├── up.* / down.* / reset.*    # cycle de vie du cluster
├── kcli.*                     # outils CLI Kafka dans broker-1
├── run.*                      # compiler + lancer (app / seed)
└── watch-group.*              # repartition des partitions entre instances
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows. Config + helpers `isValid`/`amountOf`/`explodeItems` sont fournis dans le starter.)*

---

## 5. La boîte à outils stateless

| Opérateur | Effet | Clé préservée |
|---|---|---|
| `filter` / `filterNot` | garder / exclure selon un prédicat | **oui** |
| `mapValues` | transformer la valeur | **oui** |
| `flatMapValues` | une valeur → **0, 1 ou N** valeurs | **oui** |
| `selectKey` / `map` | changer la clé | **non** → repartition |
| `split()` | router en **branches** | **oui** |
| `merge` | fusionner deux flux | **oui** |

> Tout ce qui touche à la **clé** peut déclencher une **repartition** (un topic interne `...-repartition`) : à éviter sans raison.

**Router avec `split()`** (l'ancien `branch()` a disparu en 4.0) : `split()` ouvre un `BranchedKStream` ; chaque `branch(prédicat, Branched)` capture les enregistrements (prédicats évalués **dans l'ordre**), et `defaultBranch(Branched)` attrape le reste (sans lui, les non-appariés sont **perdus**). `Branched.withConsumer(ks -> ks.to("..."))` décrit l'action terminale d'une branche.

**Dead-letter** : `filterNot(isValide).to("...-rejected")` isole les messages invalides **sans bloquer** le traitement ni perdre de données.

**Éclater** : `flatMapValues` transforme une valeur en plusieurs (ici une commande → N lignes `orderId;sku`), en **conservant la clé**.

> **Note Kafka 4.x** : `branch()` (tableau) et `transform*` ont été retirés → `split()`/`Branched` et `process*`.

---

## 6. Atelier guidé (~50 min)

> Cluster démarré, les 5 topics créés. Le code à écrire est dans `OrderRouterApp.java` : **les 3 étapes y sont décrites, à vous de les coder** (helpers fournis).

### Étape 1 — Valider (dead-letter)

Envoyez les lignes invalides vers `orders-rejected` (`filterNot` + `isValid`), et gardez un flux `valid` des lignes valides (`filter` + `isValid`).

### Étape 2 — Router par montant avec `split()`

Sur `valid`, ouvrez un `split()` : une branche `amountOf(...) >= 100` → `orders-priority`, et la branche par défaut → `orders-standard`.

### Étape 3 — Éclater en articles

Sur `valid`, appliquez `flatMapValues(explodeItems)` → `order-items` (une ligne `orderId;sku` par article).

### Étape 4 — Lancer et produire

```bash
./run.sh app            # votre topologie (continu)
# autre terminal :
./run.sh seed           # 7 lignes : 5 valides, 2 malformees
```

### Étape 5 — Vérifier chaque sortie, puis passer à l'échelle

```bash
# Verifier les 4 sorties (attendus : priority=2, standard=3, items=9, rejected=2)
for t in orders-priority orders-standard order-items orders-rejected; do
  echo "== $t =="
  docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic "$t" --from-beginning --timeout-ms 4000
done

# Lancer une 2e instance (autre terminal) et observer le rebalance
./run.sh app
./watch-group.sh        # les 3 partitions se repartissent entre les 2 instances
```

---

## 7. Défis

### Défi 1 — Recléer et mesurer le coût

`selectKey((k, line) -> currencyOf(line))` change la clé → Streams crée un **topic de repartition interne**. Repérez-le : `kafka-topics --list` (suffixe `-repartition`). C'est pourquoi on évite `selectKey` sans raison.

### Défi 2 — Router puis fusionner

Collectez les branches de `split()` dans une `Map` (avec `Branched.as("nom")`), puis recombinez deux branches avec `merge` pour écrire un flux unifié.

### Défi 3 — Compter par branche

Insérez un `peek((k, v) -> ...)` (opération **sans effet de bord** sur le flux) pour journaliser ou compter les enregistrements traversant chaque branche — utile à l'observabilité.

> **La suite — Séance 11 :** le **traitement avec état** — `groupByKey`, `count`, `aggregate` et les **state stores** sauvegardés dans des changelogs. On quitte le pur transit pour le calcul à mémoire.

---

## 8. Dépannage

### Kafka Streams / Maven

| Symptôme | Solution |
|---|---|
| `mvn` introuvable / `release 21 not supported` | Maven absent ou JDK < 21 (voir § 2). |
| Une sortie est vide | Le topic existe-t-il (les **5** créés) ? L'app tourne-t-elle ? Avez-vous lancé `./run.sh seed` ? Consumer avec `--from-beginning` ? |
| `orders-standard` reçoit tout, `orders-priority` rien | L'ordre des prédicats dans `split()`, ou le seuil `>= 100.0` (le seeder a `o-1` et `o-3` ≥ 100). |
| Des commandes valides disparaissent | `defaultBranch` manquant : les enregistrements non appariés sont perdus. |
| Tout part dans `orders-rejected` | `isValid` reçoit la **valeur** (`(k, v) -> isValid(v)`), pas la clé. |
| `InconsistentGroupProtocolException` (2e instance) | Ancien état local : `rm -rf /tmp/kafka-streams/tp10-order-router` puis relancez. |
| Comportement étrange au redémarrage | Streams garde un **état local** `/tmp/kafka-streams/<application.id>` (voir `reset`). |

### Cluster / Docker / Windows

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (29092/8080) | Un autre kit tourne (S1–S9). `docker compose ls` puis `docker compose down`. |
| `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous `(healthy)`. |
| `... .ps1 cannot be loaded ...` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 app`. |
| Sous **Git Bash**, chemins malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1`. |

> **macOS Apple Silicon** : images Kafka multi-arch (natif). **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix run.sh`.

---

## 9. Commandes utiles (mémo)

```
# Cluster + 5 topics (Linux/macOS)
./up.sh | .\up.ps1 | docker compose up -d
for t in orders orders-rejected orders-priority orders-standard order-items; do \
  docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic "$t" --partitions 3 --replication-factor 3; done

# Windows PowerShell : 5 topics
foreach ($t in "orders","orders-rejected","orders-priority","orders-standard","order-items") { \
  docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic $t --partitions 3 --replication-factor 3 }

# Application + production + verification
./run.sh app                 | .\run.ps1 app                 # votre topologie (continu)
./run.sh app solution        | .\run.ps1 app solution        # corrige
./run.sh seed                | .\run.ps1 seed
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic order-items --from-beginning --timeout-ms 5000

# Parallelisme (consumer group = application.id)
./watch-group.sh             | .\watch-group.ps1
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group tp10-order-router
```

---

## Annexe — Cluster identique aux séances précédentes

L'infrastructure (3 brokers KRaft, `localhost:29092` côté hôte, `broker-N:9092` en interne, Kafbat UI) est la même qu'aux séances précédentes ; seul le nom de projet change (`tp-kafka-s10`). Cette séance reste en `String` (ni Schema Registry ni Connect). Pour l'adressage hôte/conteneur, voir le README de la S5.
