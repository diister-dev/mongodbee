# Cas d'usage : extraire la base Diivento pour le poste de dev

Statut : mesuré le 2026-10-08 sur les vrais schémas de `new_backend` (36 migrations, 72 cibles annotées ou inférées), branche d'intégration `claude/privacy-pseudonymisation-review`, sans patch local. Le test `library/test/privacy/usecase/diivento-like.test.ts` rejoue la même forme en réduit.

Le besoin : « tout anonymiser, garder les relations, données fausses ». Une base de dev où chaque référence joint encore, où les copies d'une même valeur coïncident, où les index uniques tiennent et où la chaîne de migrations de `develop` repart, sans qu'aucune valeur réelle ne survive.

## 1. Annoter

Tout se déclare dans les schémas de l'application, avec `@diister/mongodbee/privacy`.

| Ce qu'on a | Ce qu'on écrit | Exemple Diivento |
|---|---|---|
| une collection dont chaque document est une personne | `_id: personId("user")` | `+users`, `participant` (`{ of: ["user", "accountless_identity"] }`) |
| un `_id` en `refId` (pas `dbId`) qui est une personne | métadonnée `{ kind: "person", of: [] }` posée à la main (il n'y a pas de `personId` pour `refId`) | `accountless_identity` |
| une valeur qui doit coïncider partout pour une même personne | `personal(s, { role: "direct", space: "email", consistent: "person" })` | `+users.email`, `accountless_identity.email`, `+emails.to`, `contact.identity.email` |
| un identifiant stocké en `v.string()` | `personal(s, { role: "technical", treatment: { extract: "remap" } })` | `+emails.recipientUserId`, `expo_program_registration.registeredBy` |
| du vocabulaire de la plateforme sur lequel on joint | `notPersonal(s, raison)` | `field_definition.key`, `participant_role.roleKey`, `FieldValueSchema.t` |
| un enregistrement libre `{t, v, o}` | `dynamic(v.record(...))` + un `resolveDynamic` qui lit `t` | `participant.fields` |

Le résolveur vit dans l'application (`src/database/privacy-dynamic.ts`) et se branche dans `mongodbee.config.ts` :

```ts
import { resolveDynamic } from "#new_backend/database/privacy-dynamic.ts";
export default defineConfig({ ..., privacy: { resolveDynamic } });
```

Il renvoie, par type de champ, la classification de `v` et de `o.ref` : `email`, `phone`, `text` sur `firstname`/`lastname` vont dans les espaces partagés avec `+users` ; `number`, `boolean`, `date`, `enum_*` sont gardés ; `ref_*` est remappé ; tout le reste est faux.

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
```

La cible doit être vide. L'extrait crée collections, validateurs et index, recalcule les champs `computed`, et baseline le registre à la dernière migration.

## 3. Ce qui sort

Mesuré sur une source générée par les outils Diivento (`seed`, `seed:showcase`, `seed:random-users`, `populate-analytics`, plus 273 emails) : 6 expositions, 214 users, 375 identités sans compte, 395 participants, 2 020 scans, 287 leads, 5 130 documents.

| Gardé | Faux, cohérent | Faux, au hasard | Supprimé ou régénéré |
|---|---|---|---|
| nombres de documents par collection, type et scope ; picklists, booléens, nombres hors documents de personne ; dates décalées ; vocabulaire déclaré | ids remappés (préfixe et forme ULID gardés, ordre gardé) ; emails identiques partout pour une même personne, uniques ; scopes et noms d'instances | toute chaîne non déclarée (posture stricte) : noms d'organisation, textes libres, `devSnapshot`, `ciphertext`, hashes de mot de passe | clés hors schéma ; feuilles non classées (180 dans `flow`) |

Vérifié sur la cible : mêmes comptes, 0 document hors validateur, emails uniques, aucun document ne garde sa propre valeur personnelle, aucun email ni nom source dans un texte plus long, même secret donne la même base, autre secret une autre, l'API Diivento démarre dessus (`/api/health`), ses getters retrouvent un participant par `personRef.userId` et paginent les scans.

## 4. Limites rencontrées

- **Les prénoms et noms ne coïncident pas** entre `+users`, `accountless_identity` et `participant.fields.*.v`, malgré le même espace et `consistent: "person"`. La génération dépend de la clé de la feuille et du schéma local, pas seulement de la graine. Les emails coïncident.
- **Les références dans les charges non typées cassent** : `permissions.*.value` (`v.any()`), `content.params`. Un id `expo_organization:…` y est faux, plus remappé. 7 chemins, 122 jointures perdues.
- **Le strict faux tout le vocabulaire non déclaré** : 701 chemins Diivento, dont 289 dans des documents sans personne (flows, gabarits de badge, rôles). `notPersonal(_id, raison)` sur un type ne les garde pas en strict ; il faut annoter champ par champ. Les clés de permission (`expositions.read`) sortent fausses, donc les droits de la base extraite ne fonctionnent pas.
- **Personne ne peut se connecter** : les hashes de mot de passe sont faux. Recette à écrire : un `privacy.recompute` qui pose un hash connu.
- **Les données de dev Diivento violent leurs propres index uniques** (le mode `auto` ne crée pas les index composés des types scopés). L'extrait refuse, bruyamment et sans rien laisser : c'est le bon comportement, mais la source doit être propre.
- **Les noms faux sortent du même dictionnaire que les vrais** : un « Michel » réel peut réapparaître comme faux d'une autre personne. Pas de lien, mais un scan « aucune valeur verbatim » sur les prénoms donne de faux positifs.
- Toute la base est chargée en mémoire.
