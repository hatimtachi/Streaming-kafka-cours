# TP — Séance 5 : Consumer groups, rebalance & commit des offsets

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Java 21** · **Maven**
> Fonctionne sous **Windows**, **macOS** et **Linux**.

On approfondit le couple producteur / consommateur. À la fin de ce TP, vous saurez **router avec la clé** (ordre par utilisateur), faire **se partager les partitions** par plusieurs consommateurs d'un même groupe, **provoquer et observer un rebalance** en direct, et **maîtriser le commit des offsets** (auto vs manuel → garanties *at-least-once* / *at-most-once*).

> Le code Java tourne **sur votre machine** (via Maven) et se connecte au cluster en **`localhost:29092`**. Le cluster (3 brokers) tourne dans Docker.

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
> Plusieurs terminaux seront nécessaires (un par consommateur + un pour l'observation).

---

## 2. Démarrage rapide

```bash
# 1. Le cluster 3 brokers
./up.sh                 # ou .\up.ps1   ou   docker compose up -d
docker compose ps       # attendez 3 brokers (healthy)

# 2. Le topic de l'atelier : 3 partitions, répliqué 3 fois
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic events --partitions 3 --replication-factor 3

# 3. Compléter puis lancer le producteur, et lancer des consommateurs
./run.sh producer       # (après avoir complété le TODO ; sinon ./run.sh producer solution)
./run.sh consumer       # dans 1, puis 2, puis 3 terminaux (même group.id)
```

UI : **http://localhost:8080**.

> **Adressage (important) :** depuis votre **machine** (CLI ou code Java) → **`localhost:29092`**. Depuis **un conteneur** (le script `kcli`, l'UI) → **`broker-1:9092`**. C'est pour ça que la création du topic ci-dessus utilise `broker-1:9092` (elle s'exécute *dans* broker-1).

---

## 3. Contenu du dossier

```
tp-seance-5/
├── docker-compose.yml         # cluster 3 brokers + Kafka UI
├── pom.xml                    # projet Maven : kafka-clients
├── src/main/java/fr/esgi/kafka/tp5/
│   ├── EventProducer.java     # >>> à compléter (étape 1) <<<
│   ├── GroupConsumer.java     # fonctionne (auto-commit) ; TODO étape 5 (commit manuel)
│   └── solution/              # corrigés (EventProducer, GroupConsumer manuel)
├── src/main/resources/simplelogger.properties
├── up.* / down.* / reset.*    # cycle de vie du cluster
├── kcli.*                     # outils CLI Kafka dans broker-1
├── run.*                      # compiler + lancer une appli Java
└── watch-group.*              # observer l'assignation & le LAG en boucle
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows.)*

---

## 4. Rappels express

- **La clé décide la partition** : `partition = murmur2(clé) % nbPartitions`. Même clé → même partition → **ordre garanti par clé** (jamais à l'échelle du topic). Clé nulle → réparti (pas d'ordre métier).
- **Dans un groupe** : **1 partition → au plus 1 consommateur**. Le débit d'un groupe plafonne donc au **nombre de partitions** (3 ici → 3 consommateurs utiles au maximum, le 4ᵉ reste oisif).
- **Rebalance** : quand un consommateur rejoint, part ou tombe, les partitions sont **réassignées** entre les membres vivants.
- **Commit** : l'offset committé = le **prochain** message à lire (stocké dans `__consumer_offsets`). Committer **après** traitement = *at-least-once* ; **avant** = *at-most-once*.

---

## 5. Atelier guidé (~45 min)

> Cluster démarré, topic `events` créé (§ 2). **Gardez un terminal `watch-group` ouvert** en permanence :
> ```bash
> ./watch-group.sh        # ou .\watch-group.ps1
> ```
> Il affiche, toutes les 2 s, l'assignation (CONSUMER-ID par partition) et le LAG.

### Étape 1 — Compléter le producteur

Dans `src/main/java/fr/esgi/kafka/tp5/EventProducer.java`, suivez les `// TODO` : clé = `userId` (u1..u5), 30 messages. Puis :

```bash
./run.sh producer        # .\run.ps1 producer
```

*(Pour voir tout de suite le résultat attendu : `./run.sh producer solution`.)*

### Étape 2 — Un seul consommateur

```bash
./run.sh consumer        # .\run.ps1 consumer
```

Dans le terminal `watch-group`, vérifiez qu'**un seul consommateur détient P0, P1 et P2**.

### Étape 3 — Provoquer un rebalance

Lancez un **2ᵉ** puis un **3ᵉ** consommateur (`./run.sh consumer`) dans de nouveaux terminaux — **même** `group.id`. Observez dans `watch-group` que l'assignation **se redistribue** : chacun finit avec une partition.

### Étape 4 — Le rebalance inverse

Coupez un consommateur (**Ctrl-C**). Ses partitions **repartent** vers les autres (rebalance inverse). Notez le **LAG transitoire** pendant la redistribution.

> Avec 3 partitions, lancer un **4ᵉ** consommateur le laisse **oisif** (aucune partition) — la limite « 1 partition → 1 conso ».

### Étape 5 — Passer en commit manuel

Dans `GroupConsumer.java`, appliquez les deux `// TODO` : `ENABLE_AUTO_COMMIT_CONFIG = "false"` et `consumer.commitSync()` **après** la boucle de traitement du lot. Relancez. C'est de l'**at-least-once** : si le process est coupé **avant** le `commitSync`, les messages du lot sont **rejoués** au redémarrage. *(Comparez avec la solution : `./run.sh consumer solution`.)*

---

## 6. Défi — ordre par utilisateur + LAG sous rebalance

1. **Ordre** : avec la clé = `userId`, prouvez (sortie + `watch-group`) que l'ordre tient **par utilisateur** (tous les `u1` sur la même partition, lus dans l'ordre).
2. **LAG** : produisez en continu (relancez le producteur en boucle), déclenchez un rebalance (ajout/retrait de consommateur) et **tracez le LAG** avant / pendant / après dans `watch-group`.
3. **Bonus — KIP-848** : activez le nouveau protocole côté consommateur et comparez la « douceur » du rebalance. Ajoutez dans la config du consommateur :

   ```java
   props.put("group.protocol", "consumer");
   ```

   Avec ce protocole (piloté côté serveur, incrémental), les consommateurs non concernés continuent de consommer pendant le rebalance — le LAG monte moins. *(Bonus²: `group.instance.id` stable = static membership, un redémarrage bref ne déclenche pas de rebalance.)*

> **La suite — Séance 6 :** sémantiques de livraison & **exactly-once** (idempotence, transactions), et l'ouverture vers les **share groups** (consommateurs > partitions).

---

## 7. Dépannage

### Java / Maven

| Symptôme | Solution |
|---|---|
| `mvn : command not found` / `release 21 not supported` | Maven absent, ou JDK < 21. Voir § 1 (`java -version` doit afficher 21). |
| 1ʳᵉ exécution très longue | Téléchargement de `kafka-clients` depuis Maven Central. Normal, une fois. |
| `Connection to node -1 ... could not be established` | Cluster non démarré, ou mauvais port. Le code doit pointer **`localhost:29092`** ; `docker compose ps` doit montrer 3 brokers. |
| Le consommateur n'affiche rien | Le topic est vide : lancez d'abord le producteur. Avec `earliest`, il relit tout. |
| Beaucoup de logs `INFO` | Vérifiez `src/main/resources/simplelogger.properties`. |

### Rebalance / groupes

| Symptôme | Solution |
|---|---|
| `watch-group` : erreur transitoire | Un rebalance est en cours — la commande se rétablit au cycle suivant. |
| Un consommateur reste oisif | Vous avez plus de consommateurs que de partitions (3). Normal. |
| Rebalance « lent » au Ctrl-C | L'arrêt propre (LeaveGroup) est immédiat ; un **kill -9** attend le *session timeout*. |

### Cluster / Docker

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (29092/8080) | Un autre cluster tourne (S1–S4). `docker compose ls` puis `docker compose down` dans son dossier. |
| `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous `(healthy)`. Attendez puis réessayez. |

### Windows

| Symptôme | Solution |
|---|---|
| `... .ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 consumer`. |
| Sous **Git Bash**, `/opt/...` ou `broker-1:9092` malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1` (et `winpty` pour l'interactif). |

> **macOS Apple Silicon** : images Kafka multi-arch (natif). **Linux** : `permission denied docker.sock` → `sudo usermod -aG docker $USER` puis reconnexion.
> **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix run.sh` (le `.gitattributes` l'empêche côté Git).

---

## 8. Commandes utiles (mémo)

```
# Cluster
./up.sh | .\up.ps1 | docker compose up -d        ./down.sh | .\down.ps1 | docker compose down
docker compose ps

# Topic + observation du groupe
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic events --partitions 3 --replication-factor 3
docker compose exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group atelier-s5
./watch-group.sh | .\watch-group.ps1

# Code Java (sur votre machine, bootstrap localhost:29092)
./run.sh producer            | .\run.ps1 producer
./run.sh consumer            | .\run.ps1 consumer            # lancez-en 2-3 pour le rebalance
./run.sh consumer solution   | .\run.ps1 consumer solution   # corrigé (commit manuel)
```

> CLI depuis l'hôte (si Kafka est installé localement) : remplacez `docker compose exec broker-1 /opt/kafka/bin/<outil> --bootstrap-server broker-1:9092` par `<outil> --bootstrap-server localhost:29092`.

---

## Annexe — Pourquoi `localhost:29092` (et non 9092) ?

Le support du cours utilise `localhost:29092` comme point d'amorçage depuis l'hôte. Le `docker-compose.yml` mappe donc les brokers sur les ports hôte **29092 / 29093 / 29094**. À l'intérieur du réseau Docker, les brokers s'adressent par **`broker-N:9092`** (utilisé par l'UI et par le script `kcli`). Un seul broker suffit à amorcer un client : il récupère ensuite la carte complète du cluster.
