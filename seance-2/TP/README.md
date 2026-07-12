# TP — Séance 2 : Un cluster Kafka à 3 brokers

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** en mode **KRaft** (sans ZooKeeper) · 3 brokers · Kafka UI
> Fonctionne sous **Windows**, **macOS** et **Linux**.

Suite de la séance 1 : on passe d'un broker unique à un **cluster de 3 brokers**. À la fin de ce TP, vous saurez créer un **topic partitionné et répliqué**, lire la sortie de `--describe` (**leaders**, **réplicas**, **ISR**), observer la **répartition des partitions** sur les brokers, et vérifier que **les messages d'une même clé tombent toujours dans la même partition**.

> **Avant de commencer :** si le cluster de la **séance 1** tourne encore, arrêtez-le (il occupe les ports 9092/8080). Depuis le dossier `TP` de la S1 : `docker compose down`. Vérifiez avec `docker compose ls`.

---

## 1. Prérequis

| Système | Installer | Terminal conseillé | Scripts |
|---|---|---|---|
| **Windows 10/11** | Docker Desktop (backend **WSL 2** recommandé) | **PowerShell** | `.ps1` |
| **macOS** (Intel ou Apple Silicon) | Docker Desktop | Terminal (zsh) | `.sh` |
| **Linux** | Docker Engine + plugin `docker compose` | votre shell | `.sh` |

Vérifications (identiques partout) :

```
docker --version
docker compose version
```

Il faut **Docker ≥ 20.10.4**, **Compose v2**, ~3–4 Go de RAM libre (3 brokers), et les ports **9092**, **9094**, **9096** et **8080** disponibles. Aucun besoin d'installer Java ni Kafka.

> Les commandes `docker compose ...` sont **identiques** sur les trois systèmes. Seuls les scripts de confort changent (`.sh` vs `.ps1`).

---

## 2. Démarrage rapide

```bash
# Linux / macOS
./up.sh

# Windows (PowerShell)
.\up.ps1

# N'importe quel OS (sans script)
docker compose up -d
```

Puis :

```
docker compose ps          # attendez que broker-1, broker-2, broker-3 soient (healthy)
```

Et ouvrez **http://localhost:8080**. L'onglet **Brokers** doit montrer **3 brokers** (ID 1, 2, 3).

Arrêter : `./down.sh` · `.\down.ps1` · `docker compose down`.
Tout effacer : `./reset.sh` · `.\reset.ps1` · `docker compose down -v`.

---

## 3. Contenu du dossier

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | Le cluster : **3 brokers** KRaft + Kafka UI |
| `up.sh` · `up.ps1` | Démarrer |
| `down.sh` · `down.ps1` | Arrêter (données conservées) |
| `reset.sh` · `reset.ps1` | Arrêter **et** effacer les données |
| `kcli.sh` · `kcli.ps1` | Lancer un outil CLI Kafka dans le conteneur `broker-1` |
| `.gitattributes` | Force les fins de ligne (LF) — évite que les `.sh` cassent |
| `README.md` | Ce guide |

---

## 4. L'architecture en bref

**Trois brokers**, chacun avec un `NODE_ID` unique (1, 2, 3) et le **même quorum** de contrôleurs KRaft. Les partitions d'un topic se répartissent sur les brokers ; chaque partition a un **leader** et des **réplicas**.

```
              RESEAU DOCKER "tp-kafka-s2"
   +-----------------------------------------------+
   |  [ broker-1 ]   [ broker-2 ]   [ broker-3 ]   |   <- quorum KRaft (controllers)
   |     :29092         :29092         :29092       |   <- ecoute interne (DOCKER)
   +-----------------------------------------------+
       ^   ^   ^                         ^
       |   |   |                         |
  localhost localhost localhost     [ kafka-ui ] :8080
   :9092    :9094    :9096          (via broker-N:29092)
```

### Les deux façons d'adresser un broker

C'est **le** point clé de ce TP. Chaque broker expose plusieurs listeners :

| Listener | Adresse annoncée | Pour qui |
|---|---|---|
| `HOST` | `localhost:9092` / `9094` / `9096` | outils CLI lancés **depuis votre machine** |
| `DOCKER` | `broker-1:29092` / `broker-2:29092` / `broker-3:29092` | **conteneurs** du réseau (Kafka UI, et CLI lancée *dans* un conteneur) |
| `CONTROLLER` | interne, `:9093` | usage **interne KRaft** |

> **Pourquoi ça compte ?** Un producteur qui écrit dans un topic à 3 partitions doit joindre les **3 leaders**, donc les **3 brokers**. Quand vous lancez la CLI **à l'intérieur** de `broker-1` (via `docker compose exec`), `localhost` n'y désigne que `broker-1` — vous ne joindriez qu'un seul broker. Il faut donc utiliser l'adresse **réseau Docker** : **`--bootstrap-server broker-1:29092`**. Le client récupère alors la carte du cluster et parle à chaque leader (`broker-1/2/3:29092`).
>
> *(Depuis une CLI installée sur votre machine, c'est l'inverse : on utilise `localhost:9092` — les 3 ports hôte sont publiés.)*

---

## 5. Atelier guidé (~30 min)

> **Toutes les commandes ci-dessous s'exécutent dans le conteneur `broker-1`** (`docker compose exec broker-1 ...`) et utilisent l'adresse interne **`broker-1:29092`** — voir l'encadré ci-dessus. Elles sont **identiques sous Windows, macOS et Linux**.
>
> **Raccourci** (même chose, sans le préfixe `docker compose exec broker-1 /opt/kafka/bin/`) :
> - Linux/macOS : `./kcli.sh kafka-topics.sh --bootstrap-server broker-1:29092 --list`
> - Windows : `.\kcli.ps1 kafka-topics.sh --bootstrap-server broker-1:29092 --list`

### Étape 1 — Lancer le cluster (3 brokers)

```
docker compose up -d
docker compose ps
```

Attendez que `broker-1`, `broker-2`, `broker-3` soient `(healthy)`. Dans Kafka UI (**:8080**) → **Brokers** : vous voyez 3 brokers.

### Étape 2 — Créer un topic partitionné et répliqué

3 partitions, répliquées sur les 3 brokers :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --create --topic commandes --partitions 3 --replication-factor 3
```

### Étape 3 — Lire le `--describe`

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes
```

Sortie attendue (les numéros de leader peuvent varier) :

```
Topic: commandes  Partitions: 3  ReplicationFactor: 3
 Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
 Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
 Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

- **Leader** : le broker qui sert les lectures/écritures de la partition.
- **Replicas** : les brokers qui en détiennent une copie.
- **Isr** (*In-Sync Replicas*) : les réplicas à jour. Ici `Replicas = Isr` → tout est sain. *(On provoquera une panne en S3 pour voir l'ISR se réduire.)*

### Étape 4 — La répartition dans Kafka UI

Dans l'UI, ouvrez le topic `commandes`. Repérez, pour chaque partition, **le broker leader** et **les réplicas**. Constatez que les 3 leaders sont répartis sur les 3 brokers : la charge est distribuée.

### Étape 5 — Produire avec des clés

Format `clé:valeur`. La clé décide de la partition (hachage).

```
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --property parse.key=true --property key.separator=:
```

Saisissez (en répétant la même clé plusieurs fois) :

```
client-1:connexion
client-2:achat
client-1:ajout-panier
client-3:connexion
client-1:paiement
client-2:retour
```

Quittez avec **Ctrl-C**.

### Étape 6 — Vérifier : même clé → même partition

Consommez en affichant la **partition** et la **clé** de chaque message :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:29092 --topic commandes --from-beginning --property print.partition=true --property print.key=true
```

Observez que **tous les messages `client-1`** portent **le même numéro de partition**. Quittez avec **Ctrl-C**. (Vous pouvez aussi le vérifier visuellement dans l'UI, partition par partition.)

---

## 6. Défis (pour aller plus loin)

### Défi 1 — Sans clé : la répartition change

Produisez **sans clé** (messages simples), puis consommez avec `print.partition=true` :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes
```

Sans clé, Kafka **équilibre** les messages sur les partitions (sticky/round-robin) : il n'y a plus de regroupement par entité. C'est le contraste direct avec l'étape 5.

### Défi 2 — N'importe quel broker est un point d'entrée

Refaites un `--describe` en bootstrappant sur **broker-2**, puis **broker-3** :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-2:29092 --describe --topic commandes
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-3:29092 --describe --topic commandes
```

Même résultat : **tout broker** peut fournir la carte du cluster (métadonnées). C'est pourquoi on liste plusieurs brokers en `bootstrap-server` en production.

### Défi 3 — Repartitionner casse l'ordre par clé

Augmentez le nombre de partitions de 3 à 6 :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --alter --topic commandes --partitions 6
```

Produisez à nouveau des messages `client-1:...` et observez : la clé `client-1` peut désormais **changer de partition**. Pourquoi ? Le partitionnement est `hash(clé) % nombre_de_partitions` : changer `n` modifie le résultat. **Conséquence pratique : on dimensionne les partitions large dès le départ** (augmenter est simple, mais casse l'ordre par clé ; réduire est impossible).

> **La suite — Séance 3 :** durabilité et tolérance aux pannes. On **arrête un broker** et on observe l'ISR se réduire, l'élection d'un nouveau leader, le rôle des `acks` et de `min.insync.replicas`.

---

## 7. Dépannage

### Tous systèmes

| Symptôme | Cause probable / solution |
|---|---|
| `bind: address already in use` (9092/9094/9096/8080) | Un autre cluster tourne (souvent celui de la **S1**). Arrêtez-le : `docker compose down` dans son dossier, ou `docker compose ls` pour les retrouver. |
| Un seul broker `(healthy)`, les autres non | Laissez ~30–40 s : le quorum KRaft met un peu de temps à converger. Sinon : `docker compose logs broker-2`. |
| `--create` échoue : `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous démarrés. Attendez qu'ils soient `(healthy)`. |
| Produire « bloque » ou erreurs `NOT_LEADER` | Vous avez bootstrappé sur `localhost:9092` **dans** un conteneur : utilisez `broker-1:29092` (voir l'encadré § 4). |
| Kafka UI ne montre qu'un broker | Le bootstrap de l'UI doit lister les 3 (`broker-1/2/3:29092`) — déjà configuré. Rafraîchissez après que tout soit `(healthy)`. |
| Comportement étrange | Repartez propre : `reset` puis `up`. |

Logs : `docker compose logs -f broker-1` (idem `broker-2`, `broker-3`, `kafka-ui`).

### Windows

| Symptôme | Solution |
|---|---|
| `... up.ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou lancez : `powershell -ExecutionPolicy Bypass -File .\up.ps1`. |
| « Docker Desktop is starting / pipe not found » | Lancez **Docker Desktop** et attendez qu'il soit prêt. |
| Sous **Git Bash**, `broker-1:29092` ou `/opt/...` malmenés | Préférez **PowerShell**. Sinon préfixez par `MSYS_NO_PATHCONV=1`, et par `winpty` pour les commandes interactives (producteur/consommateur). |
| 3 brokers = RAM | Donnez ≥ 4 Go à Docker Desktop (Settings → Resources). |

### macOS

| Symptôme | Solution |
|---|---|
| Apple Silicon (M1/M2/M3) | Images `apache/kafka` et `kafbat/kafka-ui` **multi-arch** (arm64) : natif, sans émulation. |

### Linux

| Symptôme | Solution |
|---|---|
| `permission denied ... docker.sock` | `sudo usermod -aG docker $USER` puis reconnexion, ou préfixez par `sudo`. |
| `docker compose` introuvable | Installez le plugin Compose v2, ou utilisez `docker-compose` (avec tiret). |

> **Fins de ligne :** une erreur `bad interpreter: ^M` sur un `.sh` = fichier converti en CRLF. Le `.gitattributes` fourni l'empêche côté Git ; au besoin `dos2unix up.sh`.

---

## 8. Commandes utiles (mémo)

```
# Cycle de vie
./up.sh    | .\up.ps1    | docker compose up -d
./down.sh  | .\down.ps1  | docker compose down
./reset.sh | .\reset.ps1 | docker compose down -v
docker compose ps

# Outils Kafka (dans broker-1, adresse interne broker-1:29092)
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --list
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --property parse.key=true --property key.separator=:
docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:29092 --topic commandes --from-beginning --property print.partition=true --property print.key=true
```

> Raccourci : remplacez `docker compose exec broker-1 /opt/kafka/bin/` par `./kcli.sh ` (Linux/macOS) ou `.\kcli.ps1 ` (Windows).

---

## Annexe A — Sans Docker

Reproduire un cluster à 3 brokers **sans Docker** demande de lancer **trois** processus Kafka avec des fichiers `server.properties` distincts (chacun son `node.id`, ses ports `listeners`/`controller.quorum.voters`, son `log.dirs`). C'est faisable mais lourd : pour ce TP, **la voie Docker est recommandée**.

Pour mémoire, l'esquisse (un terminal par broker, après avoir formaté le stockage avec un **même** `cluster-id` partagé) :

```
# Generer UNE fois un identifiant de cluster partage par les 3 brokers
bin/kafka-storage.sh random-uuid

# Pour chaque broker : formater puis demarrer avec sa propre config
bin/kafka-storage.sh format -t <CLUSTER_ID> -c config/server-1.properties
bin/kafka-server-start.sh config/server-1.properties
# ... idem server-2.properties, server-3.properties (node.id et ports differents)
```

Le single-broker de la **séance 1** (Annexe A de son README) reste la version « sans Docker » la plus simple pour réviser les commandes de base.
