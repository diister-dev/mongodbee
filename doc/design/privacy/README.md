# Privacy : seeding, pseudonymisation, classification portée par le schéma

Statut : **brainstorm**, rien n'est tranché. Documents écrits le 2026-09-01 pour nourrir la discussion, pas pour être implémentés tels quels.

## Le point de départ

Le simulateur de migrations de mongodbee construit déjà une base complète en mémoire, corrélée par espaces d'identifiants, valide contre chaque schéma, puis la jette une fois la chaîne de migrations vérifiée. Trois besoins voisins restent sans outil :

1. **Seeding** : matérialiser cet état dans une vraie base plutôt que le jeter.
2. **Pseudonymisation** : sortir un extrait de production vers un poste de développement sans y sortir de données personnelles en clair.
3. **Classification** : savoir, à partir du schéma, quels champs se rapportent à une personne, de quelle nature, et ce qu'il faut en faire.

Le pipeline visé :

```
schémas + migrations ── générer ──► état synthétique ──┐
                                                       ├──► base de dev ──► chaîne de migrations develop
dump prod ── classifier ── transformer ──► extrait sûr ─┘
```

Deux sources, un puits. Le gain qui n'existe pas aujourd'hui : faire tourner la chaîne de migrations de `develop` sur des données qui ont la forme des vraies, pas seulement celle du schéma.

## Les documents

| Fichier | Contenu |
|---|---|
| [01-cadre-juridique.md](01-cadre-juridique.md) | Ce que le RGPD, les lignes directrices EDPB 01/2025, le G29, la CNIL et la CJUE demandent, lu à la source, et ce que chaque exigence implique pour un outil de schéma. |
| [02-metadonnees-de-schema.md](02-metadonnees-de-schema.md) | Le vocabulaire de métadonnées proposé : ce que le schéma sait déjà, les axes à ajouter, comment ils servent à la fois le seeding et la classification, et les forks à trancher. |

## Les décisions déjà prises ailleurs et réutilisées ici

- **Inférence par défaut, déclaration pour les exceptions, rapport comme verrou.** C'est la doctrine du chantier de corrélation des mocks. Elle s'applique telle quelle à la classification.
- **Une seule marche de schéma.** Il existe déjà trois parcours de schéma dans mongodbee et valibot-mock. La classification ne doit pas en ajouter un quatrième.
- **mongodbee fournit le mécanisme, jamais la politique.** Le registre, les bases légales, les durées et la cascade du droit à l'oubli restent au consommateur.

## État du spike (2026-09-01, non commité)

Construit dans `library/src/privacy/`, exporté sous `@diister/mongodbee/privacy`, 31 tests dans `library/test/privacy/` :

- `metadata.ts` : `personId`, `personal`, `notPersonal`, `mention`, `mirrorOf`. Le gate de snapshot garde la métadonnée privacy comme celle des index.
- `plan.ts` et `report.ts` : personnes, propriétaires, relations, niveaux, traitements par direction, findings. Un schéma sans aucune personne déclarée est une erreur.
- `pseudonym.ts`, `walk.ts`, `transform.ts` : la marche avec document en entrée et le transformateur de la direction `extract`.

Écarts avec le document 02, tranchés en construisant : la délégation se déclare, jamais inférée ; la cohérence des pseudonymes se déclare par espace (`consistent`) et non seulement par exécution ; un conteneur annoté est une feuille ; les clés absentes du schéma sont supprimées de l'extrait.

Construit ensuite, même session : `dynamic()` et le résolveur de champs dynamiques ; le contexte `ctx.newId()` et `ctx.now()` des transforms de migration (déterministe en mémoire, réel sur Mongo) ; `library/src/scenario/` avec la migration de naissance, le rejeu en mémoire et l'oracle ; les commandes `classify`, `seed` et `extract`. Le seed écrit dans une base vide et baseline le registre ; l'extrait refuse d'écrire dans sa source et refuse les chemins inconnus sans `--allow-unknown`.
