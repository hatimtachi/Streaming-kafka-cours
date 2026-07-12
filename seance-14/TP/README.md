# TP — Séance 14 · Interactive Queries & Tests

Deux briques prêtes pour le projet :
1. **Interactive Queries** — agréger les commandes par devise, puis **interroger le state
   store directement** via une petite **API REST** (`GET /count/EUR`), sans relire de topic.
2. **Tests** — vérifier la topologie avec **`TopologyTestDriver`**, **sans cluster**.

---

## Scripts prêts à l'emploi (Windows · Mac · Linux)

Depuis le dossier du TP, tout est scripté — inutile d'installer les outils Kafka côté hôte
(les topics sont créés **dans le conteneur**) :

| Action | Mac / Linux | Windows (PowerShell) |
|---|---|---|
| **Démarrer** le cluster + créer les topics | `./up.sh` | `.\up.ps1` |
| **Lancer** l'application | `./run.sh` | `.\run.ps1` |
| **Lancer les tests** (sans cluster) | `./test.sh` | `.\test.ps1` |
| Voir les **logs** des brokers | `./logs.sh` | `.\logs.ps1` |
| **Arrêter** le cluster | `./down.sh` | `.\down.ps1` |
| Arrêter + **effacer l'état** | `./down.sh clean` | `.\down.ps1 clean` |

> **Mac / Linux** : `chmod +x *.sh` une fois, puis `./up.sh`.
> **Windows** : si l'exécution est bloquée, lancez d'abord
> `Set-ExecutionPolicy -Scope Process Bypass -Force`, puis `.\up.ps1`.
> Prérequis communs : **Docker** en marche (pour le cluster) et **JDK 21 + Maven** (pour `run`/`test`).

Les commandes manuelles ci-dessous restent valables si vous préférez les taper.

---

## 1. Prérequis

- **Docker** + **Docker Compose** (v2) — pour l'app en direct (pas pour les tests)
- **JDK 21** et **Maven 3.9+**
- `curl` (ou un navigateur) pour interroger l'API

> Cluster : 3 brokers KRaft. Bootstrap : **`localhost:29092`**. API : **`localhost:7070`**.

---

## 2. Structure du projet

```
tp-s14/
├── docker-compose.yml
├── pom.xml                                   # kafka-streams + test-utils + JUnit 5
├── README.md
├── src/main/java/fr/esgi/kafka/tp14/
│   ├── CountApiApp.java                       # STARTER (TODO : Interactive Query)
│   └── solution/CountApiAppSolution.java      # corrigé
└── src/test/java/fr/esgi/kafka/tp14/
    └── CountTopologyTest.java                 # tests TopologyTestDriver (verts)
```

---

## Partie 1 — Interactive Queries

### 3. Compléter le TODO

Dans `CountApiApp.java`, le handler `/count/` doit interroger le store :

```java
ReadOnlyKeyValueStore<String, Long> store = streams.store(
        StoreQueryParameters.fromNameAndType(STORE, QueryableStoreTypes.keyValueStore()));
Long n = store.get(ccy);
```

`STORE` est le nom donné au store via `Materialized.as(...)` dans `buildTopology()`.

### 4. Lancer et interroger

```bash
docker compose up -d
kafka-topics.sh --bootstrap-server localhost:29092 --create \
  --topic orders --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server localhost:29092 --create \
  --topic count-by-currency --partitions 3 --replication-factor 3

mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp14.CountApiApp &

echo 'o-1;19.9;eur' | kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic orders
echo 'o-2;5;eur'    | kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic orders

curl http://localhost:7070/count/EUR      # -> EUR = 2
curl http://localhost:7070/count/USD      # -> USD = 0
```

La réponse vient **directement du store** (Interactive Query), sans relire de topic.

> Pendant un rebalance, le store peut être momentanément indisponible : l'API renvoie
> un **503** (`InvalidStateStoreException`) — réessayez.
> Corrigé : `-Dexec.mainClass=fr.esgi.kafka.tp14.solution.CountApiAppSolution`

### 5. (Notion) Distribué

Avec plusieurs instances, l'état est **partitionné**. `streams.queryMetadataForKey(STORE,
ccy, new StringSerializer())` indique **quelle instance** héberge la clé ; on lui relaie
la requête (RPC), en s'annonçant via `application.server` (host:port).

---

## Partie 2 — Tests (`TopologyTestDriver`)

### 6. Lancer la suite — sans aucun cluster

```bash
mvn -q test
# [INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
# [INFO] BUILD SUCCESS
```

Les tests **n'utilisent pas de broker** : `TopologyTestDriver` exécute la topologie en
mémoire, de façon synchrone. Ils couvrent :
- `compte_par_devise` — l'agrégat lu **dans le store** (`getKeyValueStore`)
- `ignore_les_lignes_invalides` — sortie vide, store vide
- `emet_dans_le_topic_de_sortie` — lecture du topic avec un **event time** contrôlé

### 7. À vous d'étendre

- Ajouter un test **fenêtré** (`pipeInput(k, v, Instant)` pour piloter le temps, puis
  `getWindowStore`), en réutilisant la logique de la S12/S13.
- Tester le **dead-letter** (S10) : une ligne invalide part dans `orders-rejected`.
- Désactiver/activer le cache (`STATESTORE_CACHE_MAX_BYTES_CONFIG`) et observer l'effet
  sur le nombre de mises à jour émises.

---

## 8. Dépannage

- **`/count/EUR` renvoie toujours `= 0`** : le **TODO** d'Interactive Query n'est pas
  complété (le handler lit `n = null`).
- **`Connection refused` sur 7070** : l'app n'est pas lancée, ou le port est pris.
- **503 au démarrage** : normal tant que l'application n'est pas `RUNNING` — réessayez.
- **Les tests échouent sur le nombre de sorties** : garder le cache désactivé
  (`STATESTORE_CACHE_MAX_BYTES_CONFIG = 0`) pour des sorties déterministes.

---

## 9. Arrêt

```bash
docker compose down
```
