# TP — Séance 3 : Réplication, durabilité & tolérance aux pannes

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** en mode **KRaft** · 3 brokers · Kafka UI
> Fonctionne sous **Windows**, **macOS** et **Linux**.

On **reprend le cluster à 3 brokers** de la séance 2, et on s'intéresse cette fois à la **fiabilité**. À la fin de ce TP, vous saurez configurer un topic **durable** (`replication-factor 3`, `min.insync.replicas=2`, producteur en `acks=all`), lire l'**ISR** dans `--describe`, et surtout : vous aurez **coupé un broker en direct** pour voir le leader basculer, l'ISR se réduire, le service continuer, puis l'ISR se reconstituer au redémarrage.

> **Avant de commencer :** si un cluster d'une séance précédente tourne (S1 ou S2), arrêtez-le — il occupe les ports 9092/8080. `docker compose ls` pour les retrouver, puis `docker compose down` dans le dossier concerné.

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

Il faut **Docker ≥ 20.10.4**, **Compose v2**, ~3–4 Go de RAM libre (3 brokers), et les ports **9092**, **9094**, **9096** et **8080** disponibles.

> Les commandes `docker compose ...` sont **identiques** sur les trois systèmes. Seuls les scripts de confort changent (`.sh` vs `.ps1`).

---

## 2. Démarrage rapide

```bash
# Linux / macOS                Windows (PowerShell)            Tout OS
./up.sh                        .\up.ps1                        docker compose up -d
```

Attendez que les 3 brokers soient `(healthy)` :

```
docker compose ps
```

Puis ouvrez **http://localhost:8080** (onglet **Brokers** → 3 brokers).
Arrêter : `down` · tout effacer : `reset`.

---

## 3. Contenu du dossier

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | Le cluster : **3 brokers** KRaft + Kafka UI (identique à la S2) |
| `up.*` · `down.*` · `reset.*` | Démarrer / arrêter / réinitialiser |
| `kcli.*` | Lancer un outil CLI Kafka dans `broker-1` |
| **`watch-isr.*`** | **Affiche l'ISR en boucle** — l'outil clé pour voir la bascule en direct |
| `.gitattributes` | Force les fins de ligne (LF) — évite que les `.sh` cassent |
| `README.md` | Ce guide |

*(`.*` = `.sh` pour Linux/macOS, `.ps1` pour Windows.)*

---

## 4. La configuration durable visée

La garantie de référence en production : **RF 3 · `min.insync.replicas=2` · `acks=all`**.

| Réglage | Où | Effet |
|---|---|---|
| `replication-factor 3` | à la création du topic | 3 copies de chaque partition, sur 3 brokers |
| `min.insync.replicas=2` | config du **topic** | une écriture exige **≥ 2 copies en phase (ISR)** |
| `acks=all` | propriété du **producteur** | le producteur attend la confirmation de tout l'ISR |

> **La garantie :** une écriture confirmée en `acks=all` n'est **jamais perdue** tant que `min.insync.replicas` est respecté. Avec RF 3 + min 2, le cluster **tolère la panne d'un broker sans aucune perte** ni interruption. Si un **deuxième** broker tombe, l'ISR passe sous 2 et Kafka **refuse** les écritures (`NOT_ENOUGH_REPLICAS`) plutôt que de risquer une perte — c'est le Défi 1.

### Rappel — adresser les brokers

À l'intérieur d'un conteneur, `localhost` ne désigne que ce conteneur. Pour joindre les **3 brokers** (donc les 3 leaders d'un topic partitionné), on utilise l'adresse réseau Docker **`broker-1:29092`** en `--bootstrap-server`. *(Depuis une CLI installée sur l'hôte, ce serait `localhost:9092`.)*

---

## 5. Atelier guidé (~30 min)

> Commandes exécutées dans `broker-1` (`docker compose exec broker-1 ...`), adresse interne `broker-1:29092` — **identiques sur les trois OS**.
> Raccourci : `./kcli.sh <outil> ...` (Linux/macOS) ou `.\kcli.ps1 <outil> ...` (Windows).
>
> **On coupera `broker-2`** (jamais `broker-1`, qui nous sert de point d'entrée CLI).

### Étape 0 — Créer le topic durable et produire en `acks=all`

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --create --topic commandes --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

Produisez quelques messages (le producteur attend la confirmation de l'ISR) :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --producer-property acks=all
```

Tapez 2–3 lignes, puis **Ctrl-C**. L'écriture passe normalement (ISR = 3 ≥ 2).

### Étape 1 — Lancer l'observateur d'ISR (dans un terminal dédié)

```bash
# Linux / macOS              Windows (PowerShell)
./watch-isr.sh               .\watch-isr.ps1
```

Vous voyez les 3 partitions avec, pour chacune, son **Leader** et son **Isr** (initialement `1,2,3` partout). **Laissez ce terminal ouvert** — il se rafraîchit toutes les 2 s.

### Étape 2 — Noter l'état initial

Dans la sortie de `watch-isr`, repérez par exemple :

```
Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

### Étape 3 — Couper `broker-2` (dans un AUTRE terminal)

```
docker compose stop broker-2
```

`stop` arrête le conteneur sans le supprimer — c'est une **panne simulée**.

### Étape 4 — Observer la bascule

Regardez le terminal `watch-isr` : en quelques secondes,

```
Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
```

→ le **leader est passé de 2 à 3**, et **le broker 2 a quitté l'ISR**. La bascule est **automatique** : seul un réplica de l'ISR peut être promu, donc aucune écriture confirmée n'est perdue.

### Étape 5 — Reproduire : ça marche encore

```
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --producer-property acks=all
```

L'écriture **passe toujours** : il reste **2 réplicas en phase** (Isr = 2 ≥ `min.insync.replicas`). La garantie tient malgré la panne.

### Étape 6 — Relancer le broker

```
docker compose start broker-2
```

Dans `watch-isr`, l'ISR **se reconstitue** (retour à `1,2,3`) une fois broker-2 rattrapé. Le cluster est revenu à l'état sain.

---

## 6. Défis (pour aller plus loin)

### Défi 1 — Une panne de trop : l'écriture est refusée

Avec `broker-2` déjà coupé, coupez **aussi `broker-3`** :

```
docker compose stop broker-3
```

L'ISR de certaines partitions tombe à **1 < 2**. Tentez de produire en `acks=all` :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --producer-property acks=all
```

Vous obtenez une erreur **`NOT_ENOUGH_REPLICAS`** : Kafka **refuse** d'écrire plutôt que de risquer une perte. C'est `min.insync.replicas=2` qui protège la donnée. Relancez ensuite : `docker compose start broker-2 broker-3`.

> **Comparez :** créez un topic `commandes-acks1` **sans** `min.insync.replicas` et produisez en `--producer-property acks=1`. Avec un broker en moins, l'écriture passe quand même — mais sans la même garantie de durabilité.

### Défi 2 — `delete` vs `compact`

Créez un topic **compacté** (ne garde que la dernière valeur par clé) :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --create --topic etat-panier --partitions 1 --replication-factor 3 --config cleanup.policy=compact

docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic etat-panier --property parse.key=true --property key.separator=:
```

Envoyez plusieurs valeurs pour une même clé :

```
panier-42:1 article
panier-42:2 articles
panier-42:3 articles
panier-7:1 article
```

La compaction conserve **la dernière valeur de chaque clé** (`panier-42 -> 3 articles`). C'est ce mécanisme qui permettra aux *changelogs* de Kafka Streams (S9+) de se reconstruire. *(La compaction tourne en arrière-plan ; sur un si petit volume elle peut être différée — l'important est de comprendre le principe.)*

### Défi 3 — Le détail des réplicas

Voyez l'ISR et les éventuels réplicas non synchronisés sous un autre angle :

```
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes --under-replicated-partitions
```

À l'état sain : aucune ligne (tout est répliqué). Coupez un broker et relancez : les partitions sous-répliquées apparaissent.

> **La suite — Séance 4 :** place au **code**. Un échauffement Java, puis les API **Producer** et **Consumer** pour écrire et lire dans Kafka par programme — fini la ligne de commande.

---

## 7. Dépannage

### Tous systèmes

| Symptôme | Cause probable / solution |
|---|---|
| `bind: address already in use` (9092/9094/9096/8080) | Un autre cluster tourne (S1/S2). `docker compose ls` puis `docker compose down` dans son dossier. |
| `--create` : `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous démarrés/sains. Attendez `(healthy)`. |
| `watch-isr` affiche « en attente du cluster » | Le cluster démarre encore, ou vous avez coupé **broker-1** (le point d'entrée CLI). Ne coupez que broker-2/broker-3. |
| Produire « bloque » / `NOT_LEADER` | Bootstrap sur `localhost:9092` **dans** un conteneur : utilisez `broker-1:29092`. |
| `NOT_ENOUGH_REPLICAS` inattendu | Il manque des brokers : l'ISR est sous `min.insync.replicas`. Relancez les brokers arrêtés. |
| Comportement étrange | `reset` puis `up`. |

Logs : `docker compose logs -f broker-1` (idem broker-2/3, kafka-ui).

### Windows

| Symptôme | Solution |
|---|---|
| `... .ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\up.ps1`. |
| `watch-isr.ps1` ne s'efface pas / s'arrête | Lancez-le dans **PowerShell** (pas via double-clic). Quittez avec Ctrl-C. |
| Sous **Git Bash**, `broker-1:29092` ou `/opt/...` malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1`, et `winpty` pour l'interactif. |
| 3 brokers = RAM | Donnez ≥ 4 Go à Docker Desktop (Settings → Resources). |

### macOS

| Symptôme | Solution |
|---|---|
| Apple Silicon (M1/M2/M3) | Images `apache/kafka` et `kafbat/kafka-ui` **multi-arch** (arm64) : natif. |

### Linux

| Symptôme | Solution |
|---|---|
| `permission denied ... docker.sock` | `sudo usermod -aG docker $USER` puis reconnexion, ou `sudo`. |
| `docker compose` introuvable | Plugin Compose v2, ou `docker-compose` (avec tiret). |

> **Fins de ligne :** `bad interpreter: ^M` sur un `.sh` = CRLF. Le `.gitattributes` l'empêche côté Git ; au besoin `dos2unix watch-isr.sh`.

---

## 8. Commandes utiles (mémo)

```
# Cycle de vie
./up.sh | .\up.ps1 | docker compose up -d        ./down.sh | .\down.ps1 | docker compose down
./reset.sh | .\reset.ps1 | docker compose down -v        docker compose ps

# Panne / reprise d'un broker (conteneur conserve)
docker compose stop broker-2
docker compose start broker-2

# Topic durable + producteur acks=all
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --create --topic commandes --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --producer-property acks=all

# Observer l'ISR
./watch-isr.sh | .\watch-isr.ps1
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes
```

> Raccourci : remplacez `docker compose exec broker-1 /opt/kafka/bin/` par `./kcli.sh ` (Linux/macOS) ou `.\kcli.ps1 ` (Windows).

---

## Annexe — Cluster identique à la séance 2

L'infrastructure (3 brokers KRaft, listeners, Kafka UI) est **la même qu'en S2** ; seul le nom de projet Compose change (`tp-kafka-s3`). Si vous avez déjà le dossier S2, vous pouvez réutiliser son `docker-compose.yml` — ce TP n'ajoute que l'outil `watch-isr` et la démarche de tolérance aux pannes. Pour le détail de l'adressage (`localhost:909X` vs `broker-N:29092`) et le « sans Docker », voir le README de la séance 2.
