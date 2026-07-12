# TP — Séance 1 : Votre premier cluster Kafka

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** en mode **KRaft** (sans ZooKeeper) · Kafka UI
> Fonctionne sous **Windows**, **macOS** et **Linux**.

À la fin de ce TP, vous aurez un cluster Kafka qui tourne sur votre machine, vous saurez **créer un topic**, **produire** et **consommer** des messages en ligne de commande, et vous aurez observé concrètement les notions vues en cours : le **log**, les **partitions** et les **offsets**.

> **Le point clé multi-OS :** tout tourne dans des conteneurs Docker. Les commandes `docker compose ...` sont **identiques** sur les trois systèmes. Les seuls éléments propres à chaque OS sont les scripts de confort (`.sh` pour Linux/macOS, `.ps1` pour Windows).

---

## 1. Prérequis

| Système | Installer | Terminal conseillé | Scripts de confort |
|---|---|---|---|
| **Windows 10/11** | Docker Desktop (backend **WSL 2** recommandé) | **PowerShell** | `.ps1` |
| **macOS** (Intel ou Apple Silicon) | Docker Desktop | Terminal (zsh) | `.sh` |
| **Linux** | Docker Engine + plugin `docker compose` | votre shell | `.sh` |

Vérifications (identiques partout) :

```
docker --version
docker compose version
```

Il faut **Docker ≥ 20.10.4**, **Compose v2**, ~2 Go de RAM libre, et les ports **9092** et **8080** disponibles. Aucun besoin d'installer Java ni Kafka pour ce TP (*Java 17+ arrivera en séance 2*).

> **Windows :** Docker Desktop doit être **lancé** avant de commencer. L'option la plus fluide est le backend **WSL 2** ; dans une console WSL, vous pouvez d'ailleurs utiliser directement les scripts `.sh`.
> **Pas de Docker du tout ?** Voir l'**Annexe A**.

---

## 2. Démarrage rapide

Choisissez **une** ligne selon votre environnement — toutes font la même chose :

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
docker compose ps          # attendez que "broker" soit (healthy)
```

Et ouvrez l'interface : **http://localhost:8080**

Pour arrêter : `./down.sh` · `.\down.ps1` · ou `docker compose down`.
Pour tout effacer : `./reset.sh` · `.\reset.ps1` · ou `docker compose down -v`.

---

## 3. Contenu du dossier

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | Le cluster : 1 broker KRaft + Kafka UI (identique sur tous les OS) |
| `up.sh` · `up.ps1` | Démarrer le cluster |
| `down.sh` · `down.ps1` | Arrêter (les données sont conservées) |
| `reset.sh` · `reset.ps1` | Arrêter **et** effacer toutes les données |
| `kcli.sh` · `kcli.ps1` | Lancer un outil CLI Kafka **dans** le conteneur broker |
| `.gitattributes` | Force les bonnes fins de ligne (LF) — évite que les `.sh` cassent |
| `README.md` | Ce guide |

---

## 4. L'architecture en bref

Un **seul broker** joue à la fois le rôle de *broker* (sert les messages) et de *controller* (gère les métadonnées via KRaft). Plus de ZooKeeper depuis Kafka 4.x.

```
   VOTRE MACHINE (hote)              RESEAU DOCKER "tp-kafka-s1"
   --------------------              ---------------------------
   Outils CLI  --- localhost:9092 ----------> [ broker ]   (broker + controller)
   Navigateur  --- localhost:8080 ---> [ kafka-ui ] -- broker:29092 --> [ broker ]
```

Le broker expose **trois listeners**, chacun pour un usage différent :

| Listener | Adresse annoncée | Pour qui |
|---|---|---|
| `HOST` | `localhost:9092` | les outils CLI lancés depuis **votre machine** |
| `DOCKER` | `broker:29092` | les **conteneurs** du réseau Docker (ici Kafka UI) |
| `CONTROLLER` | interne, `:9093` | usage **interne KRaft** (élection, métadonnées) |

> **Pourquoi deux listeners pour les clients ?** Depuis votre machine, le broker est joignable sur `localhost:9092`. Mais pour le conteneur Kafka UI, « localhost » désignerait *lui-même* — il doit donc joindre le broker par son nom de réseau Docker, `broker:29092`. Retenez : **CLI = `localhost:9092`**.

---

## 5. Atelier guidé (~30 min)

> **Les commandes ci-dessous sont écrites avec `docker compose exec broker ...`** — elles s'exécutent **dans le conteneur** et sont donc **identiques sous Windows, macOS et Linux**.
>
> **Pour raccourcir**, utilisez le helper `kcli` (même commande, sans le préfixe `docker compose exec broker /opt/kafka/bin/`) :
> - Linux/macOS : `./kcli.sh kafka-topics.sh --bootstrap-server localhost:9092 --list`
> - Windows : `.\kcli.ps1 kafka-topics.sh --bootstrap-server localhost:9092 --list`

### Étape 1 — Lancer le cluster

```
docker compose up -d
docker compose ps
```

Attendez que le broker soit `(healthy)`.

### Étape 2 — Ouvrir Kafka UI

Rendez-vous sur **http://localhost:8080**. Le cluster `local` doit apparaître. Onglet **Brokers** : un unique broker (ID 1).

### Étape 3 — Créer le topic `premier-topic`

```
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic premier-topic
```

Vérifiez sa création (CLI puis onglet **Topics** de l'UI) :

```
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic premier-topic
```

Le `--describe` indique **1 partition** et **1 réplique** : cohérent avec notre cluster mono-broker.

### Étape 4 — Produire des messages

```
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic premier-topic
```

Le curseur attend votre saisie. Tapez quelques lignes, **une par message**, en validant à chaque fois :

```
bonjour kafka
premier evenement
troisieme message
```

Quittez le producteur avec **Ctrl-C**.

### Étape 5 — Consommer depuis le début

```
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic premier-topic --from-beginning
```

Vos trois messages s'affichent, **dans l'ordre**. Le consommateur reste en écoute : ouvrez un **second terminal**, relancez le producteur (étape 4), envoyez un message — il apparaît en direct côté consommateur. Quittez avec **Ctrl-C**.

### Étape 6 — Observer partitions & offsets

Dans Kafka UI, ouvrez `premier-topic` → onglet **Messages**. Chaque message porte un **offset** (0, 1, 2, …) : sa position dans le log. C'est l'idée centrale du cours — un topic est un **journal ordonné et persistant**.

---

## 6. Défis (pour aller plus loin)

### Défi 1 — Le rejeu (replay)

Relancez exactement la commande de l'**étape 5**. Les messages **réapparaissent depuis le début**.

> Pourquoi ? Kafka **conserve** les messages (rétention) au lieu de les supprimer après lecture, contrairement à une file classique. C'est ce qui rend possibles la reprise après panne, le débogage et l'ajout de nouveaux consommateurs sur un historique existant.

### Défi 2 — Plusieurs partitions

Créez un topic à **3 partitions**, puis produisez des messages **avec une clé** :

```
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic evenements-3p --partitions 3
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic evenements-3p --property "parse.key=true" --property "key.separator=:"
```

Saisissez par exemple :

```
alice:connexion
bob:achat
alice:deconnexion
carol:connexion
```

Dans l'UI, ouvrez `evenements-3p` : les messages se **répartissent entre les 3 partitions**, et ceux d'une **même clé** (`alice`) atterrissent **toujours sur la même partition**. C'est la base du partitionnement par clé.

### Défi 3 — Lire les offsets en CLI

```
docker compose exec broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic evenements-3p
```

Comparez la somme des offsets de fin au nombre de messages produits.

> **La suite — Séance 2 :** plusieurs consommateurs qui se **partagent** les partitions (les *consumer groups*), la réplication, et un cluster à 3 brokers.

---

## 7. Dépannage

### Tous systèmes

| Symptôme | Cause probable / solution |
|---|---|
| `bind: address already in use` | Le port 9092 ou 8080 est déjà pris. Libérez-le, ou changez le mapping dans `docker-compose.yml`. |
| CLI : `Connection refused` | Le broker n'est pas encore prêt. Attendez `(healthy)` : `docker compose ps`. |
| Kafka UI « offline » | Le bootstrap doit être `broker:29092` (réseau Docker) — déjà configuré dans le compose. |
| Comportement étrange | Repartez propre : `reset` puis `up`. |

Logs : `docker compose logs -f broker` · `docker compose logs -f kafka-ui`

### Windows

| Symptôme | Solution |
|---|---|
| `... up.ps1 cannot be loaded ... execution policy` | Autorisez les scripts pour votre session : `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`. Ou lancez sans changer la politique : `powershell -ExecutionPolicy Bypass -File .\up.ps1`. |
| « Docker Desktop is starting / pipe not found » | Lancez **Docker Desktop** et attendez qu'il soit prêt avant de relancer la commande. |
| Sous **Git Bash**, chemins `/opt/...` transformés en `C:/...` | Git Bash convertit les chemins. Préférez **PowerShell** ; sinon préfixez : `MSYS_NO_PATHCONV=1 docker compose exec ...`. Pour les commandes interactives (producteur/consommateur), préfixez par `winpty`. |

### macOS

| Symptôme | Solution |
|---|---|
| Apple Silicon (M1/M2/M3) | Les images `apache/kafka` et `kafbat/kafka-ui` sont **multi-arch** (arm64) : aucune émulation, ça tourne nativement. |

### Linux

| Symptôme | Solution |
|---|---|
| `docker: permission denied ... /var/run/docker.sock` | Ajoutez votre utilisateur au groupe docker (`sudo usermod -aG docker $USER`) puis reconnectez-vous, ou préfixez par `sudo`. |
| `docker compose` introuvable | Installez le plugin Compose v2, ou utilisez l'ancienne commande `docker-compose` (avec tiret). |

> **Fins de ligne :** si un script `.sh` renvoie une erreur du type `bad interpreter: ^M`, c'est qu'il a été converti en CRLF. Le fichier `.gitattributes` fourni l'empêche côté Git ; au besoin, reconvertissez en LF (`dos2unix up.sh`).

---

## 8. Commandes utiles (mémo)

```
# Cycle de vie (choisir selon l'OS, ou la forme docker compose universelle)
./up.sh    | .\up.ps1    | docker compose up -d
./down.sh  | .\down.ps1  | docker compose down
./reset.sh | .\reset.ps1 | docker compose down -v
docker compose ps

# Outils Kafka (forme universelle ; raccourci kcli entre parentheses)
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <nom>
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic <nom>
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <nom> --from-beginning
```

> Raccourci équivalent : remplacez `docker compose exec broker /opt/kafka/bin/` par `./kcli.sh ` (Linux/macOS) ou `.\kcli.ps1 ` (Windows).

---

## Annexe A — Sans Docker (cluster Kafka local)

Si Docker n'est pas disponible, lancez Kafka 4.3.0 directement, en KRaft mono-nœud. Depuis le dossier d'installation de Kafka (commandes `bin/...` sous Linux/macOS, `bin\windows\...` sous Windows) :

```bash
# Linux / macOS
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

```bat
:: Windows (équivalents .bat)
bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format --standalone -t <CLUSTER_ID> -c config\server.properties
bin\windows\kafka-server-start.bat config\server.properties
```

Le broker écoute alors sur `localhost:9092` ; les outils s'utilisent **sans** `docker compose exec` (directement depuis `bin/`). Pour l'interface, lancez Kafbat UI séparément et pointez-la sur `localhost:9092`.
