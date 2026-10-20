# Ajouter des serveurs

On peut commencer sur un seul serveur et en ajouter plus tard, sans
réécrire ni migrer les données. Ce document dit, pour 1, 2, 3 et 4
serveurs ou plus, quels fichiers utiliser, ce qu'on gagne, et ce qu'il
faut faire côté base de données à chaque étape.

## La base de données ne change pas de schéma

Ajouter des serveurs **ne demande aucune migration SQL**. Les migrations
(`database/migrations/`) suivent les versions de GoFlow, pas le nombre de
machines : on les applique à chaque mise à jour de GoFlow, comme
d'habitude, quel que soit le déploiement.

Ce que chaque étape demande côté Postgres, c'est de l'infrastructure :

| Passage | Côté base de données |
|---|---|
| 1 → 2 serveurs | Un rôle de réplication et une ligne d'autorisation sur le serveur actuel. Les données ne bougent pas. |
| 2 → 3 serveurs (cluster) | Une copie de la base dans le cluster (`pg_dump` / `pg_restore`), GoFlow arrêté le temps de la copie. L'ancienne base reste intacte. |
| 3 → 4 serveurs et plus | Rien : on ajoute des instances GoFlow. |

## En bref

| Serveurs | Fichiers | Ce qu'on gagne | Limite |
|---|---|---|---|
| **1** | `docker-compose.yaml` | Le plus simple | La machine tombe : tout s'arrête ; la protection, ce sont les sauvegardes copiées ailleurs |
| **2** | serveur 1 : `docker-compose.yaml` ; serveur 2 : `deploy/replica-server.compose.yaml` | Une copie de la base tenue à jour en continu sur une autre machine, prête à prendre le relais | Bascule **manuelle** |
| **3** | `deploy/ha/node.compose.yaml` sur chacun, `deploy/ha/app.compose.yaml` pour GoFlow | Bascule **automatique** de Postgres, sans perte | GoFlow et Redpanda en un seul exemplaire |
| **4+** | les 3 serveurs de base + `deploy/ha/scale-ha.compose.yaml` | Plusieurs GoFlow derrière un load balancer : plus aucun point unique | — |

Pourquoi trois pour la bascule automatique : avec deux serveurs, si le
lien entre eux coupe, chacun peut se croire principal et écrire de son
côté. Le troisième arbitre (quorum etcd).

---

## 1 serveur

```bash
docker compose up -d --build --wait
```

Laisser `WRITE_PATH=direct` (le défaut) et `CITUS_ENABLED` vide.

Sauvegarder chaque nuit et **copier hors du serveur** — c'est la seule
protection contre la perte de la machine :

```bash
# cron : 0 3 * * * cd /chemin/goflow && ./scripts/backup-postgres.sh >> backups/backup.log 2>&1
rsync -a backups/ sauvegarde@autre-machine:/sauvegardes/goflow/   # ou rclone, stockage objet…
./scripts/test-restore.sh                                          # de temps en temps : la sauvegarde se restaure ?
```

---

## 2 serveurs : une copie de la base, prête à prendre le relais

Le serveur 1 garde tout (GoFlow, Postgres principal). Le serveur 2 fait
tourner une **réplique** : une copie de la base qui suit le serveur 1 en
continu, à quelques millisecondes près. Si le serveur 1 est perdu, on
promeut la réplique et on démarre GoFlow sur le serveur 2 : quasiment
aucune perte, quelques minutes d'arrêt — au lieu de restaurer la
sauvegarde de la nuit.

Dans l'exemple : serveur 1 = `10.0.0.1`, serveur 2 = `10.0.0.2`.

### Mise en place (sans arrêter GoFlow)

**Sur le serveur 1** — autoriser la réplication :

```bash
# 1. Le rôle de réplication (choisir un vrai mot de passe)
docker exec goflow-postgres psql -U goflow -d goflow \
  -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'mot-de-passe-replication'"

# 2. Autoriser le serveur 2 à répliquer, puis recharger la configuration
docker exec goflow-postgres bash -c \
  "echo 'host replication replicator 10.0.0.2/32 scram-sha-256' >> \$PGDATA/pg_hba.conf"
docker exec goflow-postgres psql -U goflow -c "SELECT pg_reload_conf()"
```

3. Rendre Postgres joignable par le serveur 2 : dans le `.env` du
   serveur 1, `POSTGRES_BIND=0.0.0.0`, puis `docker compose up -d postgres`
   (quelques secondes de coupure de la base, GoFlow se reconnecte seul).
   Au pare-feu, n'ouvrir le port 5432 **qu'au serveur 2** — jamais à
   Internet.

**Sur le serveur 2** — le même dépôt et le **même `.env`** que le
serveur 1, plus le mot de passe de réplication :

```bash
REPLICATION_PASSWORD=mot-de-passe-replication PRIMARY_HOST=10.0.0.1 \
docker compose -f docker-compose.yaml -f deploy/replica-server.compose.yaml up -d postgres
```

Au premier démarrage, la réplique copie la base du serveur 1, puis la
suit. Vérifier :

```bash
docker exec goflow-postgres psql -U goflow -d goflow -tAc "SELECT pg_is_in_recovery()"   # t : c'est une réplique
docker exec goflow-postgres psql -U goflow -d goflow -tAc "SELECT count(*) FROM process_instances"
```

Les sauvegardes continuent sur le serveur 1, comme avant.

### Si le serveur 1 est perdu : basculer sur le serveur 2

```bash
# 1. La réplique devient la base principale (elle accepte les écritures)
docker exec goflow-postgres psql -U goflow -c "SELECT pg_promote()"

# 2. Démarrer GoFlow sur le serveur 2
docker compose -f docker-compose.yaml -f deploy/replica-server.compose.yaml up -d
```

3. Envoyer les utilisateurs et les workers vers le serveur 2 (DNS, ou
   l'adresse configurée dans les applications).
4. Remettre les sauvegardes en place sur le serveur 2 (cron).

**Le serveur 1 ne doit pas revenir tel quel** : s'il redémarre avec son
ancienne base, il y aurait deux bases principales. Avant de le remettre
en service, effacer son volume Postgres et en refaire une réplique du
serveur 2 (les mêmes étapes, serveurs inversés).

Le journal d'événements Redpanda n'est pas copié sur le serveur 2. En
mode `WRITE_PATH=direct` (le défaut), il ne sert qu'à l'observation : le
moteur n'en a pas besoin pour reprendre.

Testé : la copie initiale, le suivi continu (une instance démarrée sur le
serveur 1 est sur le serveur 2 en moins d'une seconde), la promotion, et
la base promue reste principale après un redémarrage.

---

## 3 serveurs : bascule automatique (cluster Patroni)

Les trois serveurs font tourner Postgres en cluster : un principal, deux
copies synchrones (une écriture confirmée est sur deux machines). Si le
principal tombe, une copie prend le relais **toute seule** (21 à 28 s
mesurées, aucune instance perdue) ; GoFlow suit, à travers HAProxy.

Mise en place et passage depuis une base existante : voir
[Operations — Postgres high availability](operations.md#postgres-high-availability).
En résumé :

1. Appliquer les migrations sur la base actuelle, comme d'habitude.
2. Démarrer les trois nœuds (`deploy/ha/node.compose.yaml`), vérifier le cluster.
3. Arrêter GoFlow, faire un `pg_dump` de la base actuelle (une lecture).
4. Créer le rôle et la base vide dans le cluster, `pg_restore` dans le principal.
5. Démarrer GoFlow sur le cluster (`deploy/ha/app.compose.yaml`).

L'ancienne base reste arrêtée mais intacte : revenir en arrière, c'est
redémarrer GoFlow sans `app.compose.yaml`. Si on avait 2 serveurs, la
réplique du serveur 2 n'est plus utile : son serveur devient l'un des
trois nœuds (après avoir effacé son volume).

Les sauvegardes passent alors par HAProxy :
`PGHOST=haproxy PGPORT=5000 PGPASSWORD=… ./scripts/backup-postgres.sh`.

---

## 4 serveurs et plus : plusieurs GoFlow

Les trois serveurs de base restent tels quels ; on ajoute des instances
GoFlow derrière un load balancer (nginx), PgBouncer passant par HAProxy :

```bash
docker compose -f docker-compose.yaml -f docker-compose.scale.yaml \
  -f deploy/ha/app.compose.yaml -f deploy/ha/scale-ha.compose.yaml up -d --scale goflow=3
```

Rien à faire côté base de données. Toutes les tâches de fond sont sûres
avec plusieurs instances (voir
[Operations — Horizontal scaling](operations.md#horizontal-scaling)).

---

## Plus tard, pour de très gros volumes

- **`WRITE_PATH=partitioned`** (le mode « Camunda 8 », voir
  [Architecture](architecture.md#partitioned-write-path)) : utile avec
  plusieurs instances GoFlow. Pour qu'il survive à la perte d'une
  machine, Redpanda doit lui-même tourner sur trois nœuds — fichier non
  fourni aujourd'hui.
- **Citus** : seulement si un seul serveur Postgres ne suit plus en
  écriture. Il demande ses propres machines (coordinateur et workers).
