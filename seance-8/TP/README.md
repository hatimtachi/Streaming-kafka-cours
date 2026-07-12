# TP — Séance 8 : Kafka Connect

> **Stream Processing avec Apache Kafka** · ESGI · 5ᵉ année · Data Engineering
> Apache Kafka **4.3.0** (KRaft, 3 brokers) · **Schema Registry** + **Kafka Connect** Confluent **8.3.0**
> Fonctionne sous **Windows**, **macOS** et **Linux**. **Aucun code Java** : tout est déclaratif (JSON + REST).

Kafka Connect déplace des données **entre Kafka et des systèmes externes** (fichiers, bases, S3, Elasticsearch…) **sans écrire de producteur/consommateur**. On déploie une intégration en **envoyant une configuration JSON** à l'API REST du worker. Ici : une **source** lit un fichier vers un topic, un **sink** réécrit ce topic vers un fichier, et on ajoute une **transformation (SMT)** — le tout piloté par REST. Le **défi** rebranche `AvroConverter` sur le **Schema Registry de la séance 7**.

> Dernière séance de l'arc *Fondations*. À partir de la S9 : **Kafka Streams** (logique riche : agrégations, jointures, fenêtres).

---

## 1. Prérequis

| Système | Docker | Terminal | Scripts | REST |
|---|---|---|---|---|
| **Windows 10/11** | Docker Desktop (WSL 2) | **PowerShell** | `.ps1` | `curl.exe` ou `.\connect.ps1` |
| **macOS** | Docker Desktop | Terminal | `.sh` | `curl` ou `./connect.sh` |
| **Linux** | Docker Engine + Compose v2 | votre shell | `.sh` | `curl` ou `./connect.sh` |

```
docker compose version
```

> **Pas de JDK/Maven nécessaire** : Connect tourne dans un conteneur, on ne fait que lui envoyer du JSON.
> Ports requis : **29092–29094** (brokers), **8081** (Schema Registry), **8083** (Connect), **8080** (UI).
> Le premier démarrage **télécharge l'image** `cp-kafka-connect` (~1 Go) — prévoir une connexion correcte.

---

## 2. Démarrage rapide

```bash
# 1. Toute la pile (brokers + registre + worker Connect)
./up.sh                 # ou .\up.ps1   ou   docker compose up -d
docker compose ps       # 3 brokers (healthy) + schema-registry + connect

# 2. Attendre que le worker reponde (~30-60 s) et lister les plugins
curl -s localhost:8083/                 # version          (Windows : curl.exe)
./connect.sh plugins                    # doit contenir FileStreamSource / FileStreamSink

# 3. Deployer la source, verifier, consommer
./connect.sh deploy connectors/file-source.json
./connect.sh status file-source         # "state":"RUNNING"
./kcli.sh kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-connect --from-beginning --max-messages 3

# 4. Deployer le sink, lire le fichier de sortie
./connect.sh deploy connectors/file-sink.json
cat data/orders-out.txt                 # les memes lignes, via Kafka, sans code
```

> **Adressage :** API REST depuis l'hôte → `localhost:8083` (Connect) et `localhost:8081` (registre).
> À l'intérieur des **configs de connecteurs**, le registre se note `http://schema-registry:8081` (réseau Docker).

---

## 3. Contenu du dossier

```
tp-seance-8/
├── docker-compose.yml         # 3 brokers + schema-registry + connect (worker, REST 8083)
├── connectors/                # configurations JSON (deployees par REST)
│   ├── file-source.json         # fichier -> topic orders-connect (StringConverter)
│   ├── file-sink.json           # topic orders-connect -> fichier (StringConverter)
│   ├── file-source-routed.json  # source + SMT RegexRouter -> routed-orders-connect
│   ├── file-source-avro.json    # DEFI : source -> orders-avro (AvroConverter + registre)
│   └── file-sink-avro.json      # DEFI : orders-avro -> fichier (AvroConverter + registre)
├── data/                      # monte dans le conteneur connect sous /data
│   └── orders.txt               # fichier source (1 ligne = 1 commande)
├── up.* / down.* / reset.*    # cycle de vie de la pile
├── kcli.*                     # outils CLI Kafka dans broker-1
└── connect.*                  # piloter l'API REST de Connect (deploy/list/status/delete)
```

*(`.*` = `.sh` Linux/macOS, `.ps1` Windows. Pas de projet Maven : Connect est sans code.)*

---

## 4. Les idées clés

- **Déclaratif, pas du code.** Un connecteur = une **configuration JSON** (`name`, `connector.class`, paramètres, convertisseurs). On la **POST** sur l'API REST (port 8083). Aucune ligne de Java.
- **Source** (lit un système externe → publie dans Kafka) vs **Sink** (consomme un topic → écrit ailleurs). Kafka sert de tampon durable et de point de **découplage** (un topic peut alimenter plusieurs sinks).
- **Architecture** : un **worker** (processus JVM) exécute des **tasks** ; un connecteur se découpe en tasks (`tasks.max`) réparties sur les workers. En **mode distribué**, l'état (configs, offsets, status) vit dans des **topics internes** → le cluster est résilient (rééquilibrage comme les consumer groups).
- **Convertisseur** = le pont entre la représentation interne et les octets Kafka, **côté clé et côté valeur**, indépendamment. `StringConverter` / `JsonConverter` (simples, sans registre) ; **`AvroConverter`** réutilise le **Schema Registry** (paramètre `value.converter.schema.registry.url`). Changer de format ne change pas de connecteur.
- **SMT** (Single Message Transforms) : petites transformations **message par message**, sans code, chaînables via `transforms`. Ex. `RegexRouter` (renomme le topic cible), `InsertField`, `MaskField`, `ReplaceField`, `ValueToKey`. Pour de la logique riche (agrégation, jointure) → **Kafka Streams**.

| Verbe REST | Effet |
|---|---|
| `GET /connectors` | lister |
| `POST /connectors` | créer (corps `{name, config}`) |
| `GET /connectors/<nom>/status` | état + chaque task (avec la trace en cas d'échec) |
| `PUT /connectors/<nom>/config` | mettre à jour à chaud (idempotent ; corps = la config seule) |
| `DELETE /connectors/<nom>` | supprimer |
| `GET /connector-plugins` | plugins installés |

> Le script `connect.sh deploy <fichier>` est **idempotent** : il supprime puis recrée le connecteur (pratique pour itérer).

---

## 5. Atelier guidé (~45 min)

> Pile démarrée, `./connect.sh plugins` montre bien `FileStreamSource` et `FileStreamSink`.

### Étape 1 — Déployer la source

```bash
./connect.sh deploy connectors/file-source.json
./connect.sh status file-source        # connecteur + task à "RUNNING"
```

La source lit `/data/orders.txt` (côté conteneur) et publie chaque ligne dans `orders-connect`.

### Étape 2 — Confirmer l'arrivée des lignes

```bash
./kcli.sh kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-connect --from-beginning --max-messages 3
```

Les 3 lignes du fichier sont là — **sans avoir écrit de producteur**.

### Étape 3 — Déployer le sink

```bash
./connect.sh deploy connectors/file-sink.json
cat data/orders-out.txt                 # même contenu, réécrit par le sink
./connect.sh list                       # ["file-source","file-sink"]
```

Aller-retour **fichier → topic → fichier**, sans une ligne de code.

### Étape 4 — Le côté streaming (temps réel)

Ajoutez une ligne au fichier source pendant que tout tourne — elle traverse le pipeline :

```bash
printf 'o-4;7.5\n' >> data/orders.txt    # Windows : "o-4;7.5`n" | Add-Content data\orders.txt
# quelques secondes plus tard :
cat data/orders-out.txt                  # o-4;7.5 est apparu en bout de chaine
```

### Étape 5 — Ajouter un SMT (RegexRouter)

`file-source-routed.json` ajoute un `RegexRouter` qui réécrit le topic cible (`(.*)` → `routed-$1`) :

```bash
./connect.sh deploy connectors/file-source-routed.json
./kcli.sh kafka-topics.sh --bootstrap-server broker-1:9092 --list   # un topic routed-orders-connect apparait
```

Les nouvelles lignes partent désormais vers `routed-orders-connect`.

---

## 6. Défis

### Défi 1 — Avro de bout en bout (pont avec la S7)

Une source et un sink en **`AvroConverter`**, branchés sur le registre de la S7 (un même fichier produit de l'Avro, le sink le relit) :

```bash
./connect.sh deploy connectors/file-source-avro.json    # ecrit dans orders-avro (Avro)
./connect.sh deploy connectors/file-sink-avro.json      # relit orders-avro -> fichier
curl -s localhost:8081/subjects                          # orders-avro-value est apparu
cat data/orders-avro-out.txt
```

Connect **enregistre/récupère les schémas tout seul** : la source crée le schéma, le sink le retrouve. (Le `schema.registry.url` des configs pointe sur `http://schema-registry:8081`, car le convertisseur s'exécute **dans** le conteneur Connect.)

### Défi 2 — Enchaîner deux SMT

Dans une config source, chaînez plusieurs transformations : `"transforms": "route,insert"` puis définissez `transforms.insert.type` (ex. `InsertField$Value`) à la suite du `RegexRouter`, et observez l'effet combiné.

### Défi 3 — Dead Letter Queue (robustesse)

Sur un **sink**, ajoutez à la config :

```
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq-orders",
"errors.deadletterqueue.topic.replication.factor": "3"
```

Les enregistrements en échec partent vers `dlq-orders` au lieu de **stopper la task** — indispensable en production.

> **La suite — Séance 9 :** début de l'arc **Kafka Streams** — `KStream`, `KTable`, et la **topologie** de traitement (au-delà du message par message des SMT).

---

## 7. Dépannage

### Kafka Connect

| Symptôme | Solution |
|---|---|
| `./connect.sh plugins` ne montre **pas** FileStreamSource/Sink | Les connecteurs FileStream vivent dans `/usr/share/filestream-connectors` : il doit être dans `CONNECT_PLUGIN_PATH` (il l'est). Vérifiez `docker compose logs connect`. |
| `curl localhost:8083/` ne répond pas | Le worker met **30–60 s** à démarrer (scan des plugins + création des topics internes). Patientez. |
| `409 Conflict` à la création | Le connecteur existe déjà : `./connect.sh deploy ...` (idempotent) ou `./connect.sh delete <nom>` d'abord. |
| Task à l'état `FAILED` | `./connect.sh status <nom>` affiche la trace. Souvent : mauvais `connector.class`, ou `topics` (sink) vs `topic` (source) confondus. |
| `data/orders-out.txt` reste vide | Le sink n'a pas démarré, ou problème d'écriture dans `/data` (voir Linux ci-dessous). `./connect.sh status file-sink`. |
| L'image `cp-kafka-connect` ne se télécharge pas | ~1 Go : vérifiez l'espace disque et l'accès réseau. |

### Convertisseurs / Avro (défi)

| Symptôme | Solution |
|---|---|
| Sink Avro : `SerializationException` / schéma introuvable | Le topic doit avoir été **écrit en Avro** (source Avro) ; un topic `StringConverter` n'est pas lisible en Avro. Utilisez la paire `*-avro.json`. |
| `Connection refused` vers le registre | Dans les **configs de connecteurs**, l'URL est `http://schema-registry:8081` (réseau Docker), pas `localhost`. |

### Cluster / Docker

| Symptôme | Solution |
|---|---|
| `bind: address already in use` (8083 / 8081 / 29092) | Un autre kit tourne (S1–S7). `docker compose ls` puis `docker compose down`. |
| `connect` redémarre en boucle | Brokers pas `(healthy)`, ou `CONNECT_*_STORAGE_REPLICATION_FACTOR` > nombre de brokers. `docker compose logs connect`. |

### Linux — écriture dans `data/`

Le conteneur Connect tourne sous un utilisateur non-root ; s'il ne peut pas écrire `data/orders-out.txt` :

```bash
chmod -R 777 data           # solution simple pour le TP
```

### Windows

| Symptôme | Solution |
|---|---|
| `... .ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou `powershell -ExecutionPolicy Bypass -File .\connect.ps1 list`. |
| `curl` se comporte bizarrement | Sous PowerShell, `curl` est un alias d'`Invoke-WebRequest` : utilisez **`curl.exe`** ou le script **`.\connect.ps1`**. |
| Ajouter une ligne au fichier source | `"o-4;7.5`n" | Add-Content data\orders.txt` (les backticks créent le saut de ligne). |

> **macOS Apple Silicon** : images multi-arch (natif). **Fins de ligne** : `bad interpreter: ^M` = `.sh` en CRLF → `dos2unix connect.sh`.

---

## 8. Commandes utiles (mémo)

```
# Pile + plugins
./up.sh | .\up.ps1 | docker compose up -d        docker compose ps
curl -s localhost:8083/                          # (Windows : curl.exe)
./connect.sh plugins | .\connect.ps1 plugins

# Cycle de vie d'un connecteur
./connect.sh deploy connectors/file-source.json  | .\connect.ps1 deploy connectors\file-source.json
./connect.sh list                                | .\connect.ps1 list
./connect.sh status file-source                  | .\connect.ps1 status file-source
./connect.sh config file-source                  | .\connect.ps1 config file-source
./connect.sh delete file-source                  | .\connect.ps1 delete file-source

# Verifier les topics / la donnee
./kcli.sh kafka-topics.sh --bootstrap-server broker-1:9092 --list
./kcli.sh kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-connect --from-beginning --max-messages 3
cat data/orders-out.txt
```

---

## Annexe — Pile = S7 + worker Connect

L'infrastructure (3 brokers KRaft, `localhost:29092` côté hôte, `broker-N:9092` en interne, Schema Registry sur 8081, Kafbat UI) est **celle de la S7** ; on ajoute un service `connect` (image `cp-kafka-connect:8.3.0`) qui pointe ses `CONNECT_BOOTSTRAP_SERVERS` sur `broker-1/2/3:9092`, expose son API REST sur `localhost:8083`, monte `./data` sous `/data` (fichiers des connecteurs FileStream), et inclut `/usr/share/filestream-connectors` dans son `plugin.path`. Confluent Platform **8.3** correspond à Apache Kafka **4.3** (nos brokers). Pour l'adressage hôte/conteneur, voir le README de la S5 ; pour le Schema Registry, celui de la S7.
