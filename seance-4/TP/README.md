# TP — Séance 4 : Premiers pas en Java (Producer & Consumer)

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Java 21** · **Maven** · `kafka-clients`
> Fonctionne sous **Windows**, **macOS** et **Linux**.

On quitte la ligne de commande pour le **code**. À la fin de ce TP, vous aurez un projet **Maven** minimal, vous aurez écrit un **producteur** qui envoie 10 messages dans le topic `commandes`, et un **consommateur** qui les relit dans une boucle `poll` — en observant la **partition** et l'**offset** de chaque message.

> Le code Java tourne **sur votre machine** (via Maven) et se connecte au cluster Kafka en `localhost:9092`. Le cluster, lui, tourne dans Docker (les 3 brokers de la S2/S3).

---

## 1. Prérequis

Deux choses : **Docker** (pour le cluster) et **JDK 21 + Maven** (pour compiler/exécuter le code).

| Système | Docker | JDK 21 + Maven | Terminal | Scripts |
|---|---|---|---|---|
| **Windows 10/11** | Docker Desktop (WSL 2) | Adoptium Temurin 21 + Maven (ou `winget`/`scoop`) | **PowerShell** | `.ps1` |
| **macOS** | Docker Desktop | `brew install openjdk@21 maven` | Terminal | `.sh` |
| **Linux** | Docker Engine + Compose v2 | paquet `openjdk-21-jdk` + `maven`, ou **SDKMAN** | votre shell | `.sh` |

Vérifications :

```
docker compose version
java -version      # doit afficher 21
mvn -version       # Maven sur un JDK 21
```

> **SDKMAN** (macOS/Linux) est pratique : `sdk install java 21-tem` puis `sdk install maven`.
> Ports requis : **9092 / 9094 / 9096** (brokers) et **8080** (UI).

---

## 2. Démarrage rapide

```bash
# 1. Le cluster Kafka (réutilisez celui de la S2/S3 s'il tourne déjà)
./up.sh                 # ou .\up.ps1   ou   docker compose up -d

# 2. Créer le topic "commandes" (3 partitions) — une seule fois
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --create --topic commandes --partitions 3 --replication-factor 3

# 3. Compiler et lancer le producteur, puis le consommateur (sur votre machine)
./run.sh producer       # ou .\run.ps1 producer
./run.sh consumer       # ou .\run.ps1 consumer   (Ctrl-C pour arrêter)
```

> Tant que vous n'avez pas complété le code (§ 5), `./run.sh producer` affiche juste « à compléter ». Pour voir le résultat attendu tout de suite : `./run.sh producer solution` puis `./run.sh consumer solution`.

---

## 3. Contenu du dossier

```
tp-seance-4/
├── docker-compose.yml         # cluster 3 brokers + Kafka UI (identique S2/S3)
├── pom.xml                    # projet Maven : 1 dépendance (kafka-clients)
├── src/main/java/
│   ├── app/                   # >>> VOTRE code (à compléter) <<<
│   │   ├── ProducerApp.java
│   │   └── ConsumerApp.java
│   └── solution/              # corrigé de référence
│       ├── ProducerApp.java
│       └── ConsumerApp.java
├── src/main/resources/
│   └── simplelogger.properties  # logs propres (niveau WARN)
├── up.* / down.* / reset.*    # cycle de vie du cluster
├── kcli.*                     # outils CLI Kafka dans broker-1
└── run.*                      # compiler + lancer une appli Java
```

*(`.*` = `.sh` pour Linux/macOS, `.ps1` pour Windows.)*

---

## 4. Le projet Maven

Une **seule dépendance** suffit pour produire et consommer : `kafka-clients`. Le `pom.xml` fixe Java 21 et ajoute `slf4j-simple` pour des logs lisibles.

Le code est séparé en deux packages :

- **`app`** : les classes à compléter (des `// TODO`). C'est votre travail.
- **`solution`** : le corrigé, à consulter **après** avoir essayé.

> **Adressage :** le code utilise `bootstrap.servers = "localhost:9092"`. Comme il s'exécute **sur votre machine** (pas dans un conteneur), il joint le cluster via les ports hôte publiés (`localhost:9092/9094/9096`). C'est différent de la CLI lancée *dans* un conteneur (qui, elle, utilise `broker-1:29092`).

---

## 5. Atelier guidé (~40 min)

> Le cluster doit tourner (`./up.sh`) et le topic `commandes` exister (voir § 2, étape 2).

### Étape 1 — Ouvrir le projet

Ouvrez le dossier dans votre IDE (IntelliJ, VS Code…) ou un éditeur. Repérez `src/main/java/app/ProducerApp.java` et `ConsumerApp.java`.

### Étape 2 — Compléter `ProducerApp`

Suivez les `// TODO` : configurer les `Properties` (bootstrap, sérialiseurs, `acks=all`), créer le `KafkaProducer` en *try-with-resources*, puis envoyer 10 `ProducerRecord` (clé `client-i`, valeur `montant=i`) avec un callback qui affiche partition et offset.

### Étape 3 — Lancer le producteur

```bash
./run.sh producer        # .\run.ps1 producer
```

Sortie attendue (l'ordre varie) :

```
envoye -> p=0 off=0
envoye -> p=2 off=0
envoye -> p=1 off=0
...
Producteur termine : 10 messages envoyes.
```

Les 10 clés `client-0`…`client-9` se répartissent sur les **3 partitions** (hachage de la clé).

### Étape 4 — Compléter `ConsumerApp`

Suivez les `// TODO` : configurer (avec `group.id` et `auto.offset.reset=earliest`), s'abonner à `commandes`, puis boucler sur `poll(Duration.ofMillis(500))` en affichant partition / offset / valeur.

### Étape 5 — Lancer le consommateur

```bash
./run.sh consumer        # .\run.ps1 consumer
```

Il relit **tout** le topic (car `earliest`) :

```
p=0 off=0 key=client-3 montant=3
p=2 off=0 key=client-1 montant=1
p=1 off=0 key=client-4 montant=4
...
```

Laissez-le tourner, relancez le producteur dans un autre terminal : les nouveaux messages **arrivent en direct**. Arrêtez avec **Ctrl-C**.

### Étape 6 — Observer partition & offset

Notez que les messages arrivent **par partition**, et que dans une partition les offsets **croissent** (l'ordre par partition de la S2). Vous pouvez aussi visualiser le topic dans Kafka UI (**:8080**).

---

## 6. Défis (pour aller plus loin)

### Défi 1 — Même groupe vs nouveau groupe (le rejeu)

- **Relancez le consommateur tel quel** (`group.id = atelier-s4`) : il ne relit **rien** de neuf — il reprend après le dernier offset commité. C'est la mémoire du groupe.
- **Changez le `group.id`** (ex. `atelier-s4-bis`) dans `ConsumerApp` et relancez : avec `earliest`, il **relit tout** depuis le début. C'est le **rejeu** (la puissance du log, vue depuis le code).

### Défi 2 — Produire sans clé

Dans `ProducerApp`, envoyez la valeur **sans clé** : `new ProducerRecord<>("commandes", "montant=" + i)` (constructeur topic + valeur). Observez dans le consommateur que la répartition change : sans clé, Kafka équilibre les messages (plus de regroupement par `client-i`).

### Défi 3 — Le callback et les erreurs

Coupez un broker (`docker compose stop broker-2`) pendant que le producteur tourne en boucle, et regardez si le callback reçoit des erreurs ou si la production continue (le leader bascule — rappel S3). Relancez avec `docker compose start broker-2`.

> **La suite — Séance 5 :** les **consumer groups**. Comment plusieurs consommateurs se partagent les partitions, le *rebalancing* (et le nouveau protocole KIP-848), et le **commit manuel** des offsets.

---

## 7. Dépannage

### Java / Maven

| Symptôme | Solution |
|---|---|
| `mvn : command not found` | Maven n'est pas installé / pas dans le PATH. Voir § 1. |
| `release version 21 not supported` | Votre JDK est < 21. `java -version` doit afficher 21. (SDKMAN : `sdk use java 21-tem`.) |
| 1ʳᵉ exécution très longue | Maven télécharge `kafka-clients` et ses dépendances depuis Maven Central. Normal, une seule fois. |
| `Connection to node -1 ... could not be established` | Le cluster n'est pas démarré, ou le topic n'existe pas. Vérifiez `docker compose ps` et créez `commandes` (§ 2). |
| Beaucoup de logs `INFO` | Vérifiez que `src/main/resources/simplelogger.properties` est présent (niveau `warn`). |
| Le consommateur « ne fait rien » | Avec `latest` et aucun nouveau message, c'est normal. Utilisez `earliest`, ou produisez en parallèle. |

### Cluster / Docker

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (9092/8080) | Un autre cluster tourne (S1/S2/S3). `docker compose ls` puis `docker compose down` dans son dossier. |
| `commandes` : `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous `(healthy)`. Attendez puis réessayez. |

### Windows

| Symptôme | Solution |
|---|---|
| `... run.ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 producer`. |
| Sous **Git Bash**, `/opt/...` ou `broker-1:29092` malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1` (et `winpty` pour l'interactif). |

### macOS / Linux

| Symptôme | Solution |
|---|---|
| Apple Silicon | Images Kafka multi-arch (arm64) : natif. |
| `permission denied ... docker.sock` (Linux) | `sudo usermod -aG docker $USER` puis reconnexion. |

> **Fins de ligne :** `bad interpreter: ^M` = `.sh` en CRLF. Le `.gitattributes` l'empêche côté Git ; au besoin `dos2unix run.sh`.

---

## 8. Commandes utiles (mémo)

```
# Cluster
./up.sh | .\up.ps1 | docker compose up -d        ./down.sh | .\down.ps1 | docker compose down
docker compose ps

# Topic (3 partitions)
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --create --topic commandes --partitions 3 --replication-factor 3
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes

# Code Java (sur votre machine)
./run.sh producer            | .\run.ps1 producer            # votre code
./run.sh consumer            | .\run.ps1 consumer
./run.sh producer solution   | .\run.ps1 producer solution   # corrigé
mvn -q compile                                               # juste compiler
mvn -q compile exec:java -Dexec.mainClass=app.ProducerApp    # forme complète
```

---

## Annexe — Réutiliser le cluster S2/S3

L'infrastructure Docker (3 brokers KRaft + Kafka UI) est **la même qu'en S2/S3**. Si ce cluster tourne déjà, **inutile de démarrer celui-ci** : votre code Java se connecte à `localhost:9092` quel que soit le cluster actif (un seul peut occuper le port 9092 à la fois). Ce TP n'ajoute, par rapport à S2/S3, que le **projet Maven** et le script `run`.
