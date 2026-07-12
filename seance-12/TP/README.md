# TP — Séance 12 · Fenêtrage & jointures

Construire une topologie qui (1) **compte les commandes par devise sur des fenêtres
d'une minute** (avec *grace period*) et (2) **enrichit chaque commande** par un taux de
change issu d'une **table de référence** (jointure `KStream`–`KTable`).

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
> `kafka-streams` est sur **Maven Central**.

---

## 2. Démarrer le cluster et créer les topics

```bash
docker compose up -d
docker compose ps               # 3 brokers "healthy"

for t in orders rates orders-converted orders-windowed-count; do
  kafka-topics.sh --bootstrap-server localhost:29092 \
    --create --topic $t --partitions 3 --replication-factor 3
done
```

**Tous en 3 partitions** : c'est la condition de **co-partitionnement** de la jointure.
Les topics internes (`-repartition`, `-changelog`) sont créés automatiquement.

- `orders` : flux `orderId;amount;currency` (ex. `o-1;19.9;eur`)
- `rates`  : table de référence, **clé = devise**, valeur = taux (ex. `EUR` → `1.0`)

---

## 3. Structure du projet

```
tp-s12/
├── docker-compose.yml                          # 3 brokers KRaft
├── pom.xml                                      # Java 21 + kafka-streams
├── README.md
└── src/main/java/fr/esgi/kafka/tp12/
    ├── WindowAndJoin.java                         # STARTER (TODO)
    └── solution/
        └── WindowAndJoinSolution.java             # corrigé
```

---

## 4. Étape 1 — Table de référence + reclé

**TODO 1** : lire `rates` en `KTable`, rekeyer `orders` par devise.

```java
KTable<String, String> rates =
        builder.table("rates", Consumed.with(Serdes.String(), Serdes.String()));
KStream<String, String> ordersByCcy = orders.selectKey((k, v) -> currencyOf(v));
```

La clé `devise` est commune à la fenêtre **et** à la jointure. `isValid`, `currencyOf`,
`amountOf` et `convert` sont fournies.

---

## 5. Étape 2 — Comptage fenêtré

**TODO 2** : compter par devise sur des fenêtres d'une minute (+ grace 10 s).

```java
ordersByCcy.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(
                Duration.ofMinutes(1), Duration.ofSeconds(10)))
        .count(Materialized.as("orders-windowed-count-store"))
        .toStream()
        .map((wk, n) -> KeyValue.pair(
                wk.key() + "@" + wk.window().start(), String.valueOf(n)))
        .to("orders-windowed-count", Produced.with(Serdes.String(), Serdes.String()));
```

La clé du résultat est un `Windowed<String>` : on la convertit en `devise@début`.

> Astuce démo : réduire la fenêtre à `Duration.ofSeconds(20)` pour la voir tourner vite.

---

## 6. Étape 3 — Jointure `KStream`–`KTable`

**TODO 3** : enrichir chaque commande par le taux de sa devise.

```java
ordersByCcy.join(rates, (order, rate) -> convert(order, rate))
        .to("orders-converted", Produced.with(Serdes.String(), Serdes.String()));
```

Pour chaque commande, la jointure lit le taux **courant** de la table.

---

## 7. Étape 4 — Lancer, alimenter, observer

```bash
mvn -q compile exec:java -Dexec.mainClass=fr.esgi.kafka.tp12.WindowAndJoin &

# La table de reference : devise -> taux (producteur en mode cle)
printf 'EUR:1.0\nUSD:0.92\nGBP:1.15\n' | \
  kafka-console-producer.sh --bootstrap-server localhost:29092 \
  --topic rates --property parse.key=true --property key.separator=:

# Produire des commandes
kafka-console-producer.sh --bootstrap-server localhost:29092 --topic orders
> o-1;19.9;eur
> o-2;50;usd
> o-3;30;gbp

# Sortie 1 : comptages par devise et par fenetre
kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic orders-windowed-count --from-beginning --property print.key=true
# Sortie 2 : commandes converties (orderId;DEVISE;montant;base)
kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic orders-converted --from-beginning
# -> o-2;USD;50.0;46.0   (50 x 0.92)
```

> Bloqué ? Corrigé : `-Dexec.mainClass=fr.esgi.kafka.tp12.solution.WindowAndJoinSolution`

---

## 8. Étape 5 — Une table vivante

```bash
# Republier le taux USD
printf 'USD:0.95\n' | kafka-console-producer.sh \
  --bootstrap-server localhost:29092 --topic rates \
  --property parse.key=true --property key.separator=:
# Produire une nouvelle commande USD -> elle utilise 0.95 (pas les anciennes)
```

La table est **vivante** : seules les commandes **suivantes** voient le nouveau taux.

---

## 9. Le défi (bonus)

1. **`suppress`** — n'émettre que le résultat **final** de chaque fenêtre :
   `.suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))` avant `toStream()`.
2. **`leftJoin`** — remplacer `join` par `leftJoin` pour conserver les commandes dont la
   devise n'a pas de taux (gérer `rate == null` dans le `ValueJoiner`).
3. **Session** — remplacer `TimeWindows` par
   `SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30))`.
4. **GlobalKTable** — lire `rates` en `GlobalKTable` (plus de co-partitionnement requis).

---

## 10. Dépannage

- **`orders-converted` vide** : avec un `join` (inner), une commande dont la devise n'a
  **pas** de taux n'est pas émise. Charger `rates` d'abord, ou tester `leftJoin`.
- **Aucune jointure** : co-partitionnement — `orders` et `rates` doivent avoir le **même
  nombre de partitions** (3).
- **`rates` lue comme un flux vide** : le producteur doit être en mode clé
  (`--property parse.key=true --property key.separator=:`).
- **Fenêtres invisibles** : produire des commandes étalées dans le temps, et lire avec
  `--property print.key=true`.

---

## 11. Arrêt

```bash
docker compose down            # -v pour supprimer aussi l'etat et les topics internes
```
