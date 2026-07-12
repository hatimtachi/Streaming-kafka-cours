# TP — Séance 7 : Sérialisation Avro & Schema Registry

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Schema Registry** Confluent **8.3.0** · **Java 21** · **Maven**
> Fonctionne sous **Windows**, **macOS** et **Linux**.

Un message Kafka n'est qu'une **suite d'octets** : producteur et consommateur ont besoin d'un **contrat**. On structure une commande `Order` en **Avro**, on l'enregistre dans un **Schema Registry**, puis on **fait évoluer** le schéma (ajout d'un champ) **sans casser** les consommateurs. À la fin, vous saurez produire/consommer de l'Avro via `KafkaAvroSerializer`, lire le registre en REST, et manier les modes de compatibilité (`BACKWARD`, `FORWARD`, `FULL`).

> Le **Schema Registry n'est pas Apache Kafka** : c'est un composant **Confluent** qui se branche en client sur les brokers. On garde donc nos brokers `apache/kafka` et on ajoute juste l'image `cp-schema-registry`.

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

> Ports requis : **29092 / 29093 / 29094** (brokers), **8081** (Schema Registry), **8080** (UI).
> Le premier `mvn compile` télécharge `kafka-avro-serializer` depuis le **dépôt Confluent** (déjà configuré dans le `pom.xml`) — prévoyez une connexion Internet.

---

## 2. Démarrage rapide

```bash
# 1. Cluster + Schema Registry
./up.sh                 # ou .\up.ps1   ou   docker compose up -d
docker compose ps       # 3 brokers (healthy) + schema-registry

# 2. Le topic des commandes
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders --partitions 3 --replication-factor 3

# 3. Le registre repond (liste vide au depart)
curl -s localhost:8081/subjects        # -> []   (Windows : curl.exe -s ...)

# 4. Produire en Avro, inspecter le registre, consommer
./run.sh producer                # apres les TODO ; sinon ./run.sh producer solution
./registry.sh                    # resume du registre (sujets, versions, mode)
./run.sh consumer                # ou ./run.sh consumer solution
```

> **Adressage :** machine (CLI/Java) → brokers `localhost:29092`, registre `http://localhost:8081`.
> Conteneurs (registre, UI, `kcli`) → brokers `broker-N:9092` (réseau Docker interne).

---

## 3. Contenu du dossier

```
tp-seance-7/
├── docker-compose.yml         # 3 brokers KRaft + service schema-registry (8081)
├── pom.xml                    # Maven : kafka-clients + kafka-avro-serializer (depot Confluent)
├── src/main/java/fr/esgi/kafka/tp7/
│   ├── AvroOrderProducer.java   # >>> a completer <<< (KafkaAvroSerializer + GenericRecord)
│   ├── AvroOrderConsumer.java   # >>> a completer <<< (KafkaAvroDeserializer)
│   └── solution/...
├── src/main/resources/avro/
│   ├── order-v1.avsc            # orderId, amount, currency (optionnel)
│   └── order-v2.avsc            # + status (defaut "NEW")  -> evolution BACKWARD
├── src/main/resources/simplelogger.properties
├── up.* / down.* / reset.*    # cycle de vie (cluster + registre)
├── kcli.*                     # outils CLI Kafka dans broker-1
├── run.*                      # compiler + lancer (producer / consumer, v1 / v2)
└── registry.*                 # interroger le Schema Registry en REST
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows.)*

---

## 4. Les idées clés

- **Un schéma = un contrat.** Sans lui, un champ renommé ou retiré casse les consommateurs en silence. Avro le rend explicite, validé et **évolutif**.
- **Avro** : binaire compact, schéma `.avsc` **séparé** du message, excellentes règles d'évolution. Un champ **optionnel** = union `["null","string"]` avec `"default": null` (le `null` en premier).
- **Schema Registry** : service REST qui stocke/versionne les schémas, attribue un **ID** à chacun, et applique la compatibilité. Trame Confluent = **`0x00`** (magic byte) + **ID** (4 octets) + **payload Avro**. Sujets par défaut : `<topic>-value` et `<topic>-key`.
- **Côté code, c'est indolore** : on remplace le sérialiseur par `KafkaAvroSerializer` et on pointe `schema.registry.url`. Le schéma est **auto-enregistré** au premier envoi si la compatibilité passe.
- **Évolution** : en `BACKWARD` (défaut), on ajoute un champ **avec défaut** et on met à jour **les consommateurs d'abord**.

| Mode | Lecture | Changements permis | Mettre à jour |
|---|---|---|---|
| **BACKWARD** *(défaut)* | nouveau lit l'ancien | supprimer ; ajouter **avec défaut** | consommateurs |
| **FORWARD** | ancien lit le nouveau | ajouter ; supprimer (avec défaut) | producteurs |
| **FULL** | les deux | uniquement champs avec défaut | n'importe |
| **NONE** | — | tout (aucun contrôle) | à éviter |

> Les variantes *transitives* vérifient **toutes** les versions, pas seulement la dernière. Kafka Streams (S9+) n'accepte que `BACKWARD`.

---

## 5. Atelier guidé (~45 min)

> Cluster + registre démarrés, topic `orders` créé, `curl localhost:8081/subjects` → `[]`.
> Code à compléter : `AvroOrderProducer.java` puis `AvroOrderConsumer.java` (le helper `loadSchema` est fourni).

### Étape 1 — Compléter le producteur Avro

Dans `AvroOrderProducer` : (TODO 1a) `VALUE_SERIALIZER_CLASS_CONFIG = KafkaAvroSerializer`, (TODO 1b) `"schema.registry.url" = "http://localhost:8081"`, (TODO 1c) construire un `GenericRecord` à partir du `schema` chargé (`orderId`, `amount`, `currency`).

### Étape 2 — Produire et inspecter le registre

```bash
./run.sh producer                # produit 5 commandes en Avro (v1)
./registry.sh subjects                              # -> ["orders-value"]
./registry.sh subjects/orders-value/versions/1      # le schema v1 stocke
```

Un `kafka-console-consumer` **classique** n'afficherait que des octets (magic byte + ID + binaire) : le sens vient du schéma, pas du message seul.

### Étape 3 — Compléter le consommateur

Dans `AvroOrderConsumer` : (TODO 3a) `VALUE_DESERIALIZER_CLASS_CONFIG = KafkaAvroDeserializer`, (TODO 3b) `"schema.registry.url"` + `"specific.avro.reader" = "false"`, (TODO 3c) afficher les champs décodés.

```bash
./run.sh consumer                # affiche orderId / amount / currency
```

### Étape 4 — Faire évoluer le schéma (v2)

`order-v2.avsc` ajoute `status` **avec un défaut `"NEW"`** (donc compatible `BACKWARD`). Produisez en v2 :

```bash
./run.sh producer v2                                # auto-enregistre la v2 si compatible
./registry.sh subjects/orders-value/versions        # -> [1,2]
```

### Étape 5 — Re-consommer : v1 **et** v2 se lisent

```bash
./run.sh consumer
```

Le **même** consommateur lit les messages des deux versions **sans planter** — c'est tout l'intérêt du registre. Avec `GenericRecord`, chaque message est décodé avec **son** schéma d'écriture : les messages v1 n'ont pas de `status` (affiché `(absent)`), les v2 l'ont. Pour qu'un ancien message **hérite** du défaut `"NEW"`, il faut un schéma de **lecture** v2 → c'est `SpecificRecord` (défi 3).

---

## 6. Défis

### Défi 1 — Casser (puis réparer)

Tentez un changement **incompatible** : ajoutez un champ **requis sans défaut** (ou changez `amount` de `double` à `string`) dans un `order-v3.avsc`, et produisez. Le registre répond **`409 Conflict`** et refuse — il **protège les consommateurs**. Ajoutez un défaut → l'enregistrement passe.

### Défi 2 — Passer en FORWARD

Changez le mode du sujet et observez quels changements deviennent permis :

```bash
# PUT du mode de compatibilite (curl.exe sous Windows)
curl -s -X PUT -H "Content-Type: application/json" --data '{"compatibility":"FORWARD"}' localhost:8081/config/orders-value
./registry.sh config/orders-value
```

En `FORWARD`, on met à jour **les producteurs d'abord** et les règles s'inversent (on peut retirer un champ à défaut).

### Défi 3 — SpecificRecord (classe Java typée)

Générez une classe `Order` depuis le `.avsc` avec `avro-maven-plugin` et utilisez-la à la place de `GenericRecord` : sécurité de type à la compilation, et le **défaut est appliqué** à la lecture des anciens messages.

> **La suite — Séance 8 :** **Kafka Connect** — brancher des sources et des sinks externes (bases, fichiers, services) **sans code**, en réutilisant ces convertisseurs de schéma (`AvroConverter`).

---

## 7. Dépannage

### Schema Registry / Avro / Maven

| Symptôme | Solution |
|---|---|
| `Could not find io.confluent:kafka-avro-serializer` | Le dépôt Confluent doit être dans le `pom.xml` (il l'est). Vérifiez l'accès à `packages.confluent.io`. |
| `curl localhost:8081/subjects` ne répond pas | Le registre démarre **après** les brokers : attendez que `docker compose ps` montre `schema-registry` lancé (~10–20 s). |
| `RestClientException ... Connection refused` (Java) | `schema.registry.url` doit être `http://localhost:8081` (le code tourne sur l'hôte). |
| `409 Conflict` à la production | Changement **incompatible** avec le mode courant : ajoutez un défaut, ou ajustez `/config/orders-value` (voir défis). |
| `Schema being registered is incompatible` | Idem : en `BACKWARD`, un nouveau champ **doit** avoir un `default`. |
| `org.apache.kafka.common.errors.SerializationException` à la lecture | Le consommateur doit utiliser `KafkaAvroDeserializer` **et** `schema.registry.url`. |

### Cluster / Docker

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (29092 / 8081 / 8080) | Un autre kit tourne (S1–S6). `docker compose ls` puis `docker compose down`. |
| `schema-registry` redémarre en boucle | Les brokers ne sont pas `(healthy)`, ou `KAFKASTORE_BOOTSTRAP_SERVERS` ne pointe pas sur `broker-N:9092`. `docker compose logs schema-registry`. |
| `replication factor 3 larger than available brokers` | Les 3 brokers ne sont pas tous prêts. |

### Windows

| Symptôme | Solution |
|---|---|
| `... .ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\run.ps1 producer`. |
| `curl` se comporte bizarrement | Sous PowerShell, `curl` est un alias de `Invoke-WebRequest`. Utilisez **`curl.exe`** pour les commandes REST, ou le script **`.\registry.ps1`**. |
| Sous **Git Bash**, `/opt/...` ou `broker-1:9092` malmenés | Préférez **PowerShell** ; sinon `MSYS_NO_PATHCONV=1`. |

> **macOS Apple Silicon** : images multi-arch (natif). **Linux** : `permission denied docker.sock` → `sudo usermod -aG docker $USER` puis reconnexion.
> **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix run.sh`.

---

## 8. Commandes utiles (mémo)

```
# Cluster + topic
./up.sh | .\up.ps1 | docker compose up -d        docker compose ps
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic orders --partitions 3 --replication-factor 3

# Produire / consommer en Avro
./run.sh producer            | .\run.ps1 producer            # v1, votre code
./run.sh producer v2         | .\run.ps1 producer v2         # v2
./run.sh consumer            | .\run.ps1 consumer
./run.sh producer solution v2 | .\run.ps1 producer solution v2

# Interroger le registre
./registry.sh                          | .\registry.ps1                          # resume
./registry.sh subjects                 | .\registry.ps1 subjects
./registry.sh subjects/orders-value/versions
curl -s localhost:8081/subjects/orders-value/versions/2     # (Windows : curl.exe)
```

---

## Annexe — Cluster = S6 + Schema Registry

L'infrastructure (3 brokers KRaft, `localhost:29092` côté hôte, `broker-N:9092` en interne, Kafbat UI) est **celle de la S6** ; on ajoute un service `schema-registry` (image `cp-schema-registry:8.3.0`) qui pointe ses `KAFKASTORE_BOOTSTRAP_SERVERS` sur `broker-1/2/3:9092` et expose son API REST sur `localhost:8081`. Confluent Platform **8.3** correspond exactement à Apache Kafka **4.3** (nos brokers). Pour le détail de l'adressage hôte/conteneur, voir le README de la S5.
