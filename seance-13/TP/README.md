# TP — Séance 13 · Données en retard, grace & horodatages

Observer **de façon déterministe** ce que deviennent les enregistrements en retard.
On embarque l'**event time** dans le message (un `TimestampExtractor` le lit), on compte
par fenêtre de 10 s avec une grace de 5 s, et on n'émet que le **résultat final**
(`suppress`). En choisissant les horodatages, on contrôle exactement la fermeture des
fenêtres — indépendamment de l'heure réelle.

---

## Scripts prêts à l'emploi (Windows · Mac · Linux)

Depuis le dossier du TP, tout est scripté — inutile d'installer les outils Kafka côté hôte
(les topics sont créés **dans le conteneur**) :

| Action | Mac / Linux | Windows (PowerShell) |
|---|---|---|
| **Démarrer** le cluster + créer les topics | `./up.sh` | `.\up.ps1` |
| **Lancer** l'application | `./run.sh` | `.\run.ps1` |
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

- **Docker** + **Docker Compose** (v2)
- **JDK 21** et **Maven 3.9+**
- Les outils CLI Kafka 4.x

> Cluster : 3 brokers KRaft. Bootstrap : **`localhost:29092`**.

---

## 2. Démarrer le cluster et créer les topics

```bash
docker compose up -d
docker compose ps               # 3 brokers "healthy"

kafka-topics.sh --bootstrap-server localhost:29092 --create \
  --topic orders-ts --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server localhost:29092 --create \
  --topic windowed-counts --partitions 3 --replication-factor 3
```

Format d'entrée : `orderId;amount;currency;eventTimeMillis`
(le **4ᵉ champ** est l'event time en millisecondes, ex. `a;1;eur;1000`).

---

## 3. Structure du projet

```
tp-s13/
├── docker-compose.yml                          # 3 brokers KRaft
├── pom.xml                                      # Java 21 + kafka-streams
├── README.md
└── src/main/java/fr/esgi/kafka/tp13/
    ├── LateDataApp.java                          # STARTER (TODO)
    └── solution/
        └── LateDataAppSolution.java              # corrigé
```

---

## 4. Étape 1 — Le `TimestampExtractor`

**TODO 1** (dans `EventTimeExtractor.extract`) : renvoyer l'horodatage embarqué.

```java
long ts = Long.parseLong(p[3].trim());
if (ts > 0) return ts;        // sinon : repli sur partitionTime
```

L'extracteur est déjà attaché à la source via `Consumed.withTimestampExtractor(...)`.

---

## 5. Étape 2 — Comptage fenêtré + `suppress`

**TODO 2** : compter par devise (fenêtre 10 s + grace 5 s), n'émettre que le final.

```java
orders.groupBy((k, v) -> currencyOf(v),
                Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(
                Duration.ofSeconds(10), Duration.ofSeconds(5)))
        .count(Materialized.as("late-count-store"))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream()
        .map((wk, n) -> KeyValue.pair(
                wk.key() + "@" + wk.window().start(), String.valueOf(n)))
        .to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
```

`suppress` ne laisse passer **qu'une** valeur par fenêtre, à sa fermeture.

---

## 6. Étape 3 — L'expérience du retard (déterministe)

Lancer l'application, puis lire la sortie dans **un autre terminal** :

```bash
mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp13.LateDataApp &

kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic windowed-counts --from-beginning --property print.key=true
```

Produire la séquence d'event times suivante (fenêtre `[0, 10000)`, grace 5 s) :

```bash
# (a) deux commandes DANS la fenetre [0,10000)
printf 'a;1;eur;1000\nb;1;eur;2000\n' | kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic orders-ts

# (b) un event time a 16000 : le stream time passe a 16000 > 10000+5000
#     => la fenetre [0,10000) FERME, suppress emet  EUR@0 -> 2
printf 'c;1;eur;16000\n' | kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic orders-ts

# (c) un RETARDATAIRE pour [0,10000), arrive APRES la fermeture => REJETE
printf 'd;1;eur;3000\n' | kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic orders-ts
```

Résultat attendu sur `windowed-counts` :

```
EUR@0    2        # emis a l'etape (b) ; d (etape c) n'est PAS compte
```

> Ce qui ferme la fenêtre, c'est l'**event time** de `c` (16000), pas le temps réel.
> Corrigé : `-Dexec.mainClass=fr.esgi.kafka.tp13.solution.LateDataAppSolution`

---

## 7. Le défi (bonus)

1. **Détourner** — avant le fenêtrage, router les retardataires extrêmes
   (event time très inférieur au stream time) vers un topic `orders-late` à retraiter.
2. **Régler** — élargir la grace à `Duration.ofSeconds(15)`, rejouer la séquence :
   `d` (event time 3000) tombe alors dans la fenêtre **encore ouverte** et **est compté**
   (`EUR@0 -> 3`).
3. **Horloge** — remplacer l'extracteur par `WallclockTimestampExtractor` (heure murale)
   et constater que le comportement dépend désormais de l'heure réelle d'arrivée.

---

## 8. Dépannage

- **Rien ne sort** : `suppress` n'émet qu'à la **fermeture** ; il faut un event time
  assez grand (l'étape *b*) pour faire avancer le stream time au-delà de `fin + grace`.
- **`d` est compté alors qu'il ne devrait pas** : la grace est peut-être trop large
  (≥ 13 s laisse `[0,10000)` ouverte pour un event time de 3000 après un stream time de 16000).
- **L'event time n'est pas pris en compte** : vérifier le **TODO 1** (l'extracteur doit
  renvoyer le 4ᵉ champ) — sinon Streams utilise l'heure de partition.
- **Une seule instance reçoit tout** : recréez les topics en 3 partitions.

---

## 9. Arrêt

```bash
docker compose down            # -v pour supprimer aussi l'etat et les topics internes
```
