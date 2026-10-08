# Cas d'usage : extraire la base Diivento pour le poste de dev

Statut : mesuré le 2026-10-08 sur les vrais schémas de `new_backend` (36 migrations, 78 cibles), branche d'intégration `claude/privacy-pseudonymisation-review` @fa17cc9, sans patch local. Les annotations Diivento sont sur la branche locale `claude/privacy-annotations`. Le test `library/test/privacy/usecase/diivento-like.test.ts` rejoue la même forme en réduit.

Le besoin : « tout anonymiser, garder les relations, données fausses ». Une base de dev où chaque référence joint encore, où les copies d'une même valeur coïncident, où les index uniques tiennent et où la chaîne de migrations de `develop` repart, sans qu'aucune valeur réelle ne survive.

## 1. Annoter

Tout se déclare dans les schémas de l'application, avec `@diister/mongodbee/privacy`.

| Ce qu'on a | Ce qu'on écrit | Exemple Diivento |
|---|---|---|
| une collection dont chaque document est une personne | `_id: personId("user")` | `+users`, `participant` (`{ of: ["user", "accountless_identity"] }`) |
| un `_id` en `refId` (pas `dbId`) qui est une personne | métadonnée `{ kind: "person", of: [] }` posée à la main (il n'y a pas de `personId` pour `refId`) | `accountless_identity` |
| une valeur qui doit coïncider partout pour une même personne | `personal(s, { role: "direct", space: "email", consistent: "person" })` | `+users.email`, `accountless_identity.email`, `+emails.to`, `contact.identity.email` |
| un identifiant stocké en `v.string()` | `personal(s, { role: "technical", treatment: { extract: "remap" } })` | `+emails.recipientUserId`, `expo_program_registration.registeredBy` |
| du vocabulaire de la plateforme sur lequel on joint | `notPersonal(s, raison)` | `participant_role.roleKey`, `member.role`, `expo_program.trackKey`, `FieldValueSchema.t` |
| un type de pure configuration, sans donnée de personne | `_id: notPersonal(id, raison, { strict: "keep" })` : tout est gardé, les ids restent remappés | flows, rôles et permissions, gabarits de badge et d'email, `field_definition`, `information` (24 cibles) |
| un champ dérivé qu'on veut recalculer | `personal(s, { role: "derived" })` + `privacy.recompute` | `auth_password.passwordHash`, `passwordSalt` |
| un enregistrement libre `{t, v, o}` | `dynamic(v.record(...))` + un `resolveDynamic` qui lit `t` | `participant.fields` |

Les deux crochets vivent dans l'application et se branchent dans `mongodbee.config.ts` :

```ts
import { resolveDynamic } from "#new_backend/database/privacy-dynamic.ts";
import { recompute } from "#new_backend/database/privacy-recompute.ts";
export default defineConfig({ ..., privacy: { resolveDynamic, recompute } });
```

Le résolveur renvoie, par type de champ, la classification de `v` et de `o.ref` : `email`, `phone`, `text` sur `firstname`/`lastname` vont dans les espaces partagés avec `+users` ; `number`, `boolean`, `date`, `enum_*` sont gardés ; `ref_*` est remappé ; tout le reste est faux.

Le `recompute` pose sur chaque `auth_password` le hash argon2 d'un mot de passe de dev connu, avec les options de Diivento (`saltOptions`) et un sel fixe. `hashPassword` tire un sel aléatoire : deux extraits du même secret ne seraient plus identiques.

**Les clés logiques en `v.string()` sont le vrai travail.** Le strict faux toute chaîne non déclarée, et le moteur de droits joint sur des valeurs : `member.role` ↔ `member-role.role`, `audience.roleKey` ↔ `expo_role.key`. Une seule oubliée suffit à vider les listes d'un organisateur sur l'extrait : c'est `member.role` qui l'a montré. Le vérificateur de jointures ne voit que les ids `préfixe:ulid`, pas ces clés ; seul un appel réel à l'API les attrape.

**Les annotations doivent être gelées par une migration.** `extract` et `classify` lisent les schémas de la dernière migration, pas `src/`. Les migrations Diivento sont des instantanés (`...parent.schemas`) : sans migration de gel, les annotations sont invisibles. Une migration sans opération suffit :

```ts
import { schemas } from "#new_backend/database/schemas.ts";
export default migrationDefinition(id, "privacy_annotations", { parent, schemas, migrate: (m) => m.compile() });
```

## 2. Lancer

```bash
cd projects/new_backend
deno task db classify                       # strict par défaut, lit la dernière migration ; 0 erreur exigé
export PRIVACY_SECRET=...                    # garder le secret = extraits reproductibles
deno task db extract --from-db diivento_prod_copy --to-db diivento_dev_extract --secret env:PRIVACY_SECRET
deno task db extract ... --shift-days 30     # décalage explicite ; sans l'option, le strict applique un décalage tiré du secret
DATABASE_MONGO_DB=diivento_dev_extract deno task db status   # 36/36 appliquées, à jour
# connexion : l'email pseudonymisé du compte + le mot de passe de dev (PRIVACY_DEV_PASSWORD, défaut azeaze)
```

La cible doit être vide. L'extrait crée collections, validateurs et index, recalcule les champs `computed`, et baseline le registre à la dernière migration.

## 3. Ce qui sort

Mesuré sur une source construite par les outils Diivento (`scripts/privacy/build-source.sh` : `seed`, `seed:showcase`, `seed:random-users --count=200`, `populate-analytics` ×3, 333 emails, champs dynamiques variés) : 6 expositions, 214 users, 797 identités sans compte, 817 participants, 4 000 scans, 1 157 leads, 6 221 inscriptions, 15 665 documents dans 33 collections ou types. `classify` : 0 erreur, 11 avertissements de propriétaire ambigu, 440 chemins faux par la posture, 24 cibles gardées comme configuration. Un extrait prend environ 35 s.

| Gardé | Faux, cohérent | Faux, au hasard | Recalculé |
|---|---|---|---|
| comptes par collection, type et scope ; types de configuration entiers ; picklists, booléens ; vocabulaire déclaré ; dates décalées | ids remappés partout, même dans les charges non typées (préfixe, forme ULID et ordre gardés) ; emails, prénoms et noms identiques pour une même personne dans toutes les collections ; emails uniques | toute autre chaîne : textes libres, noms d'organisation, `devSnapshot`, `ciphertext`, adresses | mots de passe (hash de dev connu) ; champs `computed` |

Vérifié sur la cible (`scripts/privacy/verify-extract.ts`, `open-target.ts`, `login-probe.ts`) :

- mêmes comptes ; 41 958 références sur 103 chemins joignent comme dans la source ;
- miroirs : 20 participants ↔ users et 797 ↔ identités sans compte sur email, prénom et nom ; 120 emails de l'outbox ↔ users ;
- emails des users uniques ; 0 document hors validateur dans 32 collections ;
- aucun des 15 665 documents ne garde sa propre valeur personnelle ; aucun email ni nom source dans un texte plus long ;
- même secret : base identique ; autre secret : aucun email commun ; `--shift-days 30` : dates à +30 j exactement, jointures intactes ;
- `deno task db status` : 36/36 ; les getters Diivento retrouvent un participant par `personRef.userId` et paginent les 4 000 scans ;
- l'API Diivento démarre sur l'extrait ; l'organisateur se connecte avec son email pseudonymisé et le mot de passe de dev ; `GET /api/v1/expositions` et `GET /api/v1/expositions/:id/participants` rendent, pour chacune de ses 6 expositions, le même nombre de participants que sur la source (5, 0, 20, 22, 29, 28) ; son vrai email est refusé.

## 4. Limites

Levées depuis la première mesure : les prénoms et noms coïncident (un espace explicite partage un seul générateur) ; les ids des charges non typées sont remappés (122 jointures de permissions perdues avant) ; `{ strict: "keep" }` garde la configuration (701 chemins faux avant, 440 après) ; on se connecte à l'extrait ; `_computed` est écrit ; `resolveDynamic` et `recompute` passent par la config ; `defineConfig` accepte `privacy`.

Restantes :


- **Les clés logiques non déclarées cassent en silence.** Voir plus haut. Il reste dans les types non gardés des clés à déclarer selon l'usage (`export.datasetId`, `jobs.content.params.*`, `flow_sessions`), invisibles tant qu'aucune donnée ne les exerce.
- **`v.lazy` n'est pas classé** : les conditions d'arêtes des flows (`edges.*.data.condition`) sont vues par la marche mais absentes du plan, donc régénérées (180 feuilles). Sans effet ici (littéraux), mais une valeur libre dans une condition serait remplacée même dans un type gardé.
- **Les faux emails suivent la regex de Diivento**, pas `v.email()` : valides mais illisibles (`y@y.xv`, 120 caractères de bruit).
- **Les données de dev Diivento violent leurs propres index uniques** (le mode `auto` ne crée pas les index composés des types scopés) : l'extrait refuse, bruyamment et sans rien laisser ; la source a dû être dédoublonnée.
- **Les noms faux sortent du même dictionnaire que les vrais** : un « Michel » réel peut réapparaître comme faux d'une autre personne. Pas de lien, mais un scan verbatim sur les prénoms donne des faux positifs.
- **La migration de gel importe les schémas vivants** (`#new_backend/database/schemas.ts`) : pratique pour itérer, mais elle change avec `src/`. À figer en instantané avant de la committer.
- Toute la base est chargée en mémoire.
