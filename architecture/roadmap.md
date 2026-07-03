# 🗺️ Plan d'Amélioration Architectural : GoFlow vs Camunda 8

Ce document définit la feuille de route technique pour transformer GoFlow d'un moteur monolithique SQL performant en une plateforme d'orchestration de processus distribuée à ultra-haut débit, capable de rivaliser avec ou de dépasser Camunda 8 (Zeebe).

---


## 🏗️ 1. Révolution de l'Architecture de Stockage (Le plus critique)
*Objectif : Éliminer le goulot d'étranglement de la base de données relationnelle unique pour atteindre un débit de millions d'événements par seconde.*

- **Migration vers un Log Append-Only**
  - Abandonner définitivement les opérations coûteuses de mise à jour (`UPDATE`) et de suppression (`DELETE`) sur les tables transactionnelles actives (`executions`, `jobs`).
  - Traiter chaque transition d'état comme un événement immuable, écrit de manière purement séquentielle sur le disque.
- **Intégration d'un Moteur Clé-Valeur Embarqué (LSM-Tree)**
  - Remplacer le stockage d'état temporaire en base par des moteurs natifs Go comme **BadgerDB** ou **Pebble** (équivalents de RocksDB en Go).
  - Profiter de la vitesse d'écriture brute des structures LSM (Log-Structured Merge-tree) sur les disques modernes (NVMe).
- **Séparation Stricte des Responsabilités (CQRS)**
  - **Moteur (Écriture) :** Gère uniquement le flux d'événements et maintient l'état minimal requis en mémoire vive et dans BadgerDB.
  - **PostgreSQL (Lecture/Historique) :** Devient un simple consommateur asynchrone du flux d'événements (via CDC ou workers dédiés) pour alimenter les APIs d'historique et le frontend React, sans impacter le moteur.

---

## 🌐 2. Distribution et Consensus (Horizontal Scaling)
*Objectif : Éliminer la dépendance à une machine unique (Single Point of Failure) et permettre une mise à l'échelle linéaire.*

- **Implémentation du Sharding (Partitions)**
  - Découper l'exécution des processus en plusieurs sous-ensembles indépendants (partitions).
  - Distribuer et assigner chaque nouvelle instance de processus à une partition spécifique via le hachage de son UUID.
- **Consensus Raft Natif en Go**
  - Intégrer la bibliothèque `hashicorp/raft` pour répliquer le journal d'événements de chaque partition sur plusieurs nœuds GoFlow.
  - Adopter l'architecture *Share-Nothing* : chaque partition possède sa propre goroutine unique (Single-Threaded Actor) éliminant ainsi le besoin de verrous de concurrence globaux (`lock-free`).

---

## ⚡ 3. Optimisation Réseau et Protocole (Zéro Latence)
*Objectif : Éliminer le coût CPU et réseau des requêtes HTTP/1.1 de type Polling répétitif.*

- **Support Natif du Protocole gRPC (HTTP/2)**
  - Concevoir les fichiers de spécification `.proto` calqués sur les commandes de Camunda 8 (`ActivateJobs`, `CompleteJob`, `StartProcessInstance`).
  - Permettre aux clients et SDKs officiels de Camunda 8 (Java, Go, Node.js) de communiquer nativement avec GoFlow sans modification de code.
- **Streaming gRPC / Long-Polling**
  - Remplacer le mécanisme actuel de `fetchAndLock` par des connexions serpentines gRPC (Server Streaming).
  - Pousser (*Push*) instantanément les nouveaux jobs disponibles vers les workers connectés via les *channels* Go, supprimant totalement la charge des requêtes `SELECT ... SKIP LOCKED` cycliques.


---

## 🧠 4. Optimisations In-Memory (Go Runtime)
*Objectif : Exploiter la puissance native du runtime Go et la légèreté des Goroutines.*

- **Mise en Cache Agressive des Graphes BPMN**
  - Charger l'intégralité de la structure `parsed_graph` de chaque définition de processus active dans une `sync.Map` ou un cache LRU résidant en mémoire au démarrage.
  - Bannir l'accès ou la désérialisation du champ JSONB en base de données lors de l'exécution de la fonction `executeNode()`.
- **Batching Asynchrone des Écritures de Logs**
  - Créer un tampon de logs d'audit (Buffer) adossé à un *Worker Pool* de goroutines.
  - Regrouper les écritures de logs par paquets (ex: toutes les 10ms ou par bloc de 1000 événements) avant d'exécuter un insert de masse (*Bulk Insert*) vers le stockage historique.

---

## 📋 5. Alignement Fonctionnel Standard (Parité Spécifications)
*Objectif : Garantir une compatibilité et une transition transparentes depuis les écosystèmes existants.*

- **Moteur FEEL Complet (Friendly Enough Expression Language)**
  - Faire évoluer l'évaluation CEL (actuellement partielle) vers l'intégration d'un parseur FEEL natif robuste en Go (ex: via `bbalet/feel`).
- **Gestion Avancée du Cycle de Vie des Variables**
  - Gérer l'isolation des variables (variables locales à un sous-processus vs globales à l'instance) directement dans l'arbre d'exécution en mémoire.
  - Éviter les opérations lourdes de sérialisation et de fusion JSONB lors du franchissement de passerelles parallèles ou d'activités incluses.

---

## 📊 Tableau de Synthèse des Gains Visés

| Caractéristique | Camunda 8 (Zeebe) | GoFlow Actuel | **GoFlow à Terme (Après Plan)** |
| :--- | :--- | :--- | :--- |
| **Langage / Runtime** | Java / JVM | Go / Natif | Go / Natif |
| **Empreinte RAM standard** | Élevée (8+ Go requis) | Très faible (~Mo) | Faible (Quelques dizaines de Mo) |
| **Vitesse de démarrage** | Lente (Plusieurs secondes) | Instantanée (< 50ms) | Instantanée (< 50ms) |
| **Stockage Runtime** | RocksDB (Embarqué) | PostgreSQL | BadgerDB (Embarqué) |
| **Mise à l'échelle** | Horizontale (Raft) | Verticale (Postgres) | Horizontale (Raft en Go) |
| **Protocole Principal** | gRPC | REST HTTP/1.1 | gRPC + REST HTTP/2 |
