# Métadonnées de schéma : relier, classer, générer avec le même vocabulaire

Statut : proposition de brainstorm. Les signatures sont des esquisses pour discuter, pas une API.

## 0. La thèse

Le schéma valibot est déjà le registre le plus fidèle des données de Diivento. Il sait la forme, les relations (`refId`), l'unicité (`withIndex` unique), la durée (`expireAfterSeconds`), la sémantique de certains types (`v.email()`, clés de champ). Il ne sait pas deux choses : **à qui se rapporte une donnée**, et **ce qu'il faut en faire quand elle sort de son domaine**.

La proposition : un petit vocabulaire de métadonnées, inféré autant que possible depuis ce qui existe, déclaré pour le reste, extrait par la mécanique de `withIndex`, et consommé par trois directions et un rapport :

| Direction | Entrée | Sortie | Ce que la métadonnée lui apporte |
|---|---|---|---|
| Générer | rien | document valide | qui est le sujet, quelles copies doivent coïncider, quelle cohérence de pseudonymes |
| Transformer | document réel | document pseudonymisé | quels champs, quel traitement, quelle cohérence |
| Projeter | document réel | sous-ensemble | quels champs se rapportent à ce sujet, lesquels sont fournis par lui |
| Rapporter | schéma seul | table | ce qui est certain, inféré, déclaré, inconnu |

Le même vocabulaire sert au seeding et à la classification parce que les deux répondent à la même question : **quelles valeurs doivent coïncider entre documents, et pourquoi.**

## 1. Le mécanisme existe déjà

`withIndex` dans `library/src/indexes.ts` est le précédent exact :

- un symbole de namespace (`INDEX_SYMBOL`) posé via `v.metadata({ [INDEX_SYMBOL]: ... })` dans un `v.pipe` ;
- un extracteur (`extractIndexes`) qui parcourt le schéma avec `SchemaNavigator`, calcule le chemin pointé, et lit la métadonnée sur le nœud ou dans son pipe ;
- valibot 1.4 fournit `getMetadata`, qui fusionne en profondeur toutes les actions metadata d'un pipe.

Une action `personal()` (nom à discuter) est donc une sœur de `withIndex`. Le coût est un extracteur, pas une mécanique.

Deux limites déjà connues du précédent : `walkReferences` dans le moteur de corrélation saute les schémas `lazy`, et la métadonnée posée sur un wrapper (`optional`, `union`) se lit dans le pipe du wrapper, pas de la valeur. À traiter une fois, dans la marche unique évoquée au README.

## 2. Ce que le schéma sait déjà sans déclaration

C'est le niveau zéro de l'inférence. Chaque signal existant porte déjà un fait juridique et sert déjà, ou pourrait servir, les deux directions.

| Signal existant | Fait porté | Pour générer | Pour classer et transformer |
|---|---|---|---|
| `refId(T)` | relation vers l'espace T | pool de corrélation, déjà câblé | remappage cohérent obligatoire, sinon l'extrait ne se joint plus |
| `dbId(T)` | identité du document | mint, déjà câblé | pseudonyme du document |
| `withIndex({ unique: true })` | **identifiant direct** au sens EDPB §83 | valeurs uniques dans le seed | à remplacer sans exception |
| `expireAfterSeconds` | durée de conservation, art. 5(1)(e) | dates générées dans la fenêtre | ligne du rapport art. 30(1)(f) |
| `v.email()`, `v.regex()` téléphone, `v.isoDate()` | type sémantique **certain** | faker adapté, déjà câblé | classification certaine |
| clé de champ dans `DEFAULT_SEMANTICS` (`firstname`, `phone`, `city`, `company`...) | type sémantique **probable** | faker adapté, déjà câblé | classification inférée, à confirmer |
| `v.picklist([...])` | catégorie fermée | distribution | **quasi-identifiant potentiel** (EDPB §101 cite le genre, la profession, la langue) |
| `v.string()` nu, `v.unknown()` | rien | aléatoire | **inconnu**, cas fail-closed |
| clés `ciphertext`, `dekRef`, `iv`, `authTag`, `passwordHash`, `token` | valeur opaque | ne pas générer, ou générer par le vrai chiffrement | ne pas transformer, supprimer ou rechiffrer |

Deux lignes méritent un arrêt.

`withIndex unique` est un signal que personne n'utilise encore comme tel : un champ unique par collection est, par définition, un identifiant direct de ce document. Dans `+users`, c'est `email`. L'inférence le trouve sans qu'on déclare rien.

La table `DEFAULT_SEMANTICS` de valibot-mock est un classifieur en creux. Chaque clé qui y figure, hors `description`, `summary`, `color` et `website`, désigne une donnée personnelle. Le module qui sait fabriquer un prénom sait qu'un champ en contient un. Il manque seulement de le dire.

## 3. Le vocabulaire proposé

Esquisse volontairement compacte. Chaque axe répond à un besoin nommé dans le document 01.

```ts
personal(schema, {
  role: "direct" | "quasi" | "sensitive" | "contact" | "content" | "technical",
  subject: "self" | "<chemin d'un refId dans le même document>",
  treatment: "pseudonym" | "fake" | "generalise" | "drop" | "keep" | "opaque",
  consistent: "person" | "relationship" | "transaction",
  mirrorOf: "<espace>.<chemin>",
  basis: "contract" | "legal-obligation" | "consent" | "legitimate-interest",
})

notPersonal(schema, "raison écrite")
```

### `role` : la nature du champ

| Valeur | Sens | Défaut de traitement | Source juridique |
|---|---|---|---|
| `direct` | identifie seul : email, nom complet, téléphone, identifiant externe | `pseudonym` | EDPB §83, §97 |
| `quasi` | identifie en combinaison : âge, genre, langue, fonction, ville, dates précises | `generalise` | EDPB §101 à 104 |
| `sensitive` | art. 9 : santé, opinions, religion, syndicat, orientation, biométrie | `drop` | art. 9(1) |
| `contact` | moyen de joindre, sans identifier seul : locale, préférences de notification | `keep` | minimisation 5(1)(c) |
| `content` | texte libre écrit par ou sur la personne : commentaires, notes, description | `fake` | art. 4(1), tout peut s'y trouver |
| `technical` | métadonnée de cycle de vie : statuts, compteurs, versions | `keep` | |

Le rôle porte le défaut. Le `treatment` explicite l'écrase.

### `subject` : à qui se rapporte la donnée

C'est l'axe que personne n'a, et celui qui débloque le plus.

- `self` : le document est le dossier d'une personne. `+users`, `accountless_identity`.
- un chemin : le document se rapporte à la personne désignée par ce `refId`. `participant.personRef.userId`, `scan_history.participantId`, `comment.authorId`.

Un document peut avoir plusieurs sujets. `scan_history` porte `participantId` (le scanné) et `scannedBy` (le scanneur). Ce sont deux personnes, deux sujets. Le rapport doit le montrer, et la projection art. 20 doit savoir lequel « a fourni » quoi.

Pour le seeding, `subject` est l'unité de génération. Aujourd'hui le moteur corrèle par espace : il mint des `user:...` puis fait tirer les références dans le pool. Avec `subject`, on peut générer **par personne** : N identités cohérentes, puis leurs comptes, leurs participations, leurs scans, avec un prénom qui coïncide entre `users.firstname` et `participant.fields.firstname`. C'est la troisième préoccupation de `scripts/fakegen/engine.ts` (« denormalised alignment ») que valibot-mock seul ne sait pas faire.

Pour les droits, `subject` est ce qui rend possible « tout ce qui se rapporte à `user:X` » : le parcours résout chaque `subject` et collecte. L'art. 15, l'art. 17 et l'art. 20 sont trois projections du même parcours, avec trois filtres.

### `consistent` : la politique de pseudonymes, EDPB §115 à 121

| Valeur | Clé de pool | Sens EDPB | Quand |
|---|---|---|---|
| `person` | espace seul | pseudonyme de personne, §116 | quand la même personne doit rester reliable entre expositions (compte utilisateur) |
| `relationship` | espace × scope | pseudonyme de relation, §117 | **le défaut actuel du moteur** : une identité par exposition |
| `transaction` | pas de pool | pseudonyme de transaction, §119, préféré par §121 | scans, événements, tout ce qui n'a pas besoin d'être relié |

Ce n'est pas un nouveau mécanisme. C'est la clé de pool existante, nommée avec le mot que l'EDPB emploie. Le seeding y gagne aussi : `transaction` dit au générateur qu'il n'a pas besoin de corréler ce champ, ce qui est aujourd'hui la déclaration inversée `uncorrelatedSpaces`.

### `mirrorOf` : la copie dénormalisée

`participant.fields.email` reflète `accountless_identity.email` ou `users.email`. `emails.to` reflète l'email du destinataire. `expo_organization.name` peut refléter `entreprises.name`.

Pour transformer : la copie doit recevoir **la même valeur pseudonymisée** que l'original, sinon l'extrait ne se joint plus par email et le débogage d'un flow d'inscription devient impossible. Avec `mirrorOf`, le transformateur tire la valeur de la même entrée de pool au lieu de retransformer.

Pour générer : même chose, la copie est tirée, pas régénérée. C'est aujourd'hui fait à la main dans chaque `build` de `fakegen`.

### `treatment` : ce qu'on fait de la valeur

| Valeur | Mécanique | Note |
|---|---|---|
| `pseudonym` | `HMAC(secret, valeur)` sert de graine au faker, qui produit une valeur **réaliste et valide** contre le schéma. Déterministe dans l'ensemble défini par `consistent`. | Conforme à EDPB §89 et §107 : fonction à sens unique avec clé. Pour les entrées à faible entropie (un prénom), envisager une dérivation lente, note 26. |
| `fake` | faker seedé aléatoirement, sans lien avec l'original | pour `content` : un commentaire devient un autre commentaire |
| `generalise` | réduction de précision : date vers mois, code postal vers département, âge vers tranche | exige de connaître le type ; les pipes valibot (`isoDate`, `minValue`) aident |
| `drop` | champ retiré, ou remplacé par la valeur par défaut du schéma si le champ est requis | |
| `keep` | inchangé | |
| `opaque` | jamais généré ni transformé par faker. Supprimé, ou rechiffré si une source de clé est fournie. | `ciphertext`, `iv`, `authTag`, `dekRef`, hachages argon2, jetons, ids de session |

Un point sur les `_id`. Les ULID encodent un horodatage à la milliseconde. Remapper un `_id` par un nouvel ULID minté « maintenant » casse l'ordre relatif des documents et donc les tris paginés. Le remapper en gardant l'horodatage d'origine conserve l'ordre mais fuit la date exacte. Option à discuter : un **décalage temporel global** par extrait, tiré au hasard, appliqué à tous les ULID et à toutes les dates. L'ordre et les intervalles sont préservés, l'absolu est masqué.

### `basis` : la base légale, optionnel

Uniquement pour nourrir le rapport art. 30. Pas de mécanique derrière. À ne mettre que si le rapport dérivé du schéma s'avère utile, section 6.

### `notPersonal` : l'échappatoire inversée

Même doctrine que `uncorrelatedSpaces` dans le moteur de corrélation : on déclare ce qu'on assume ne **pas** être personnel, avec une raison écrite. `company` sur une fiche participant en est le cas limite : le registre ST-01 le classe comme identifiant, `anonymize.ts` le supprime, une entreprise n'est pourtant pas une personne. La raison écrite dans le schéma vaut mieux qu'un silence.

## 4. Les champs dynamiques : la classification vit dans la donnée

`ParticipantSchema.fields` est un `v.record(v.string(), FieldValueSchema)` dont chaque valeur est `{ t: typeId, v: unknown, o: origin }`. La nature d'un champ n'est pas dans le schéma statique, elle est dans le document `FieldDefinition` de l'exposition : `key`, `typeId`, `config: v.unknown()`, `source: module | system | organizer`.

Aucune métadonnée statique ne peut classer `fields.religion` créé par un organisateur. Il faut un **résolveur fourni par le consommateur** :

```ts
resolveDynamic: (node, ctx) => Classification | SKIP
```

où `node` porte le chemin, la valeur, le document parent et le scope, et où Diivento répond en lisant `t` (un `typeId` `email` est un `direct`, un `checkbox` est `technical`) et la `FieldDefinition` correspondante (le futur drapeau `sensitive` de l'écart 9 du registre). Inconnu : `drop`, jamais `keep`.

Le point qui compte : **le seeding a besoin du même résolveur dans l'autre sens.** Générer des `fields` réalistes pour une exposition, c'est lire ses `FieldDefinition` et produire une valeur par `typeId`. Un seul hook, deux directions, exactement comme `resolve` dans valibot-mock sert aujourd'hui à la fois la corrélation et les overrides.

Cela pose une question de forme : ce résolveur doit-il pouvoir être asynchrone (lecture de `FieldDefinition` en base) ? Le moteur de population est synchrone aujourd'hui. Le plus simple est de précharger les définitions par scope avant la marche, ce que fait déjà `populateExistingMultiModelInstances` pour les instances.

## 5. Ce que le moteur de corrélation apporte déjà, et ce qui manque

Le moteur de `library/src/migration/validators/mock/correlation.ts` fait cinq phases : plan, mint, realize, populate, link. Pour la transformation, la correspondance est directe :

| Phase | Génération | Transformation |
|---|---|---|
| plan | qui possède quel espace, qui référence quoi | identique, plus la classification |
| mint | inventer les ids des propriétaires | **dériver** le pseudonyme de chaque id réel : `HMAC(secret, id)` |
| realize | tirer les noms d'instances et les `_scope` | remapper les noms de collections d'instance (`exposition:<ulid>`) |
| populate | générer chaque document | transformer chaque document, feuille par feuille |
| link | résoudre les références dans les pools | résoudre les références vers les pseudonymes |

Ce qui manque et qui est neuf :

1. Une **marche avec entrée** : le générateur de valibot-mock parcourt le schéma sans document ; il faut le même parcours avec un document en main, et un handler de feuille qui reçoit la valeur réelle. C'est la « marche unique » du README.
2. La **dérivation de graine depuis la valeur** au lieu de l'id de migration.
3. Le **rapport** : par collection, chaque chemin avec son niveau (certain, inféré, déclaré, inconnu), son rôle, son traitement, son sujet. Les inconnus sont le verrou, comme les trous de corrélation le sont aujourd'hui.

## 6. Le rapport, et ce qu'il peut nourrir

Le rapport de classification est d'abord un verrou. Il peut devenir plus.

L'art. 30(1)(c) demande les catégories de données par traitement, l'art. 30(1)(f) les délais d'effacement. Les deux sont dans le schéma dès que `role` et `expireAfterSeconds` y sont. Une section du registre Diivento (le tableau « Catégories de données traitées » de chaque fiche, et la ligne « Durée de conservation ») pourrait être **dérivée** plutôt que rédigée, et comparée à la version rédigée comme le gate compare le dernier snapshot de migration à `schemas.ts`.

Le document 01, section 7, donne l'exemple : ST-03 décrit un bloc `identity` sur `exhibitor_contact` que le schéma actuel ne porte plus. Une section dérivée ne dériverait pas.

C'est un usage secondaire. Il ne justifie pas seul le vocabulaire, mais il justifie que le rapport soit une donnée structurée et pas seulement un affichage.

## 7. Ce que ça donne sur deux schémas Diivento

Esquisse sur `+users`, pour voir la densité de déclaration. Les lignes sans commentaire sont inférées.

```ts
{
  _id: dbId("user"),                                  // identité, pseudonyme de personne
  email: withIndex(EmailSchema, { unique: true }),    // direct, certain (v.email + unique)
  firstname: v.optional(FirstNameSchema),             // direct, inféré par clé
  lastname: v.optional(LastNameSchema),               // direct, inféré par clé
  roles: v.array(refId("role")),                      // relation, keep
  status: UserStatusSchema,                           // technical, picklist
  statusReason: personal(v.string(), { role: "content" }),          // à déclarer : texte libre sur la personne
  statusChangedBy: v.optional(refId("user")),         // relation, second sujet
  picture: v.optional(PictureSchema),                 // direct (une photo identifie), à déclarer opaque ou drop
  preferredLocale: v.optional(LocaleSchema),          // quasi (langue, EDPB §101), inféré si picklist
  tags: v.optional(v.array(TagSchema)),               // content ou technical, à déclarer
  notes: v.optional(v.array(UserNoteSchema)),         // content, avec authorId = second sujet
}
```

Sur douze champs, quatre demandent une déclaration. Les huit autres sont inférés, et le rapport listerait `statusReason`, `picture`, `tags`, `notes` comme inconnus tant qu'ils ne sont pas déclarés. C'est le ratio qu'on veut : la déclaration là où le schéma ne peut pas savoir, et nulle part ailleurs.

Sur `accountless_identity` tout est inféré : `email` certain, `firstname`, `lastname`, `phone` par clé, `locale` quasi, `updatedAt` technique. Zéro déclaration.

## 8. Ce que le seeding y gagne, résumé

- **Cohérence par personne** grâce à `subject` : N personnes, puis tout ce qui s'y rattache, avec des identités qui coïncident entre collections.
- **Copies alignées** grâce à `mirrorOf`, sans code de build à la main.
- **Champs personnalisés réalistes** grâce au résolveur dynamique, lu depuis les `FieldDefinition` de l'exposition.
- **Moins de déclarations d'exclusion** : `consistent: "transaction"` remplace `uncorrelatedSpaces` pour les espaces qui n'ont pas à être reliés.
- **Champs opaques honnêtes** : générés par le vrai chiffrement quand une source de clé est fournie, omis sinon, jamais fabriqués en base64 aléatoire.

Ce que le vocabulaire ne couvre pas : la **cohérence de domaine** (énumérations pondérées, dates qui se suivent, un participant validé après avoir été créé). C'est la deuxième préoccupation de `fakegen`. Elle reste du ressort de `fake()` par champ et d'un hook de cohérence par document, hors de ce document.

## 9. Forks à trancher pour cette couche

1. **Une action ou plusieurs.** Un seul `personal({...})` porteur de tous les axes, ou des actions fines composables en pipe (`subject()`, `mirrorOf()`, `sensitive()`), à la manière de `title()` et `description()` de valibot. Je penche pour les actions fines sous un seul symbole de namespace : un pipe se lit, un objet de six clés se devine.
2. **`subject` déclaré ou inféré.** Inférer depuis le premier `refId` vers un espace « personne », avec rapport quand un document en porte plusieurs, ou exiger la déclaration sur tout document contenant du personnel. Je penche pour l'inférence avec rapport, par cohérence avec la doctrine.
3. **Où déclarer les espaces « personne ».** `user`, `accountless_identity`, `participant` sont des personnes ; `role`, `exposition` ne le sont pas. Option de session, ou métadonnée sur le `dbId` de la collection. La métadonnée sur `dbId` est plus locale et survit aux renommages.
4. **Le résolveur dynamique.** Synchrone avec préchargement, ou asynchrone avec un moteur de population réécrit. Le préchargement est moins de travail et suffit à `FieldDefinition`.
5. **Le nom.** `personal`, `pii`, `privacy`, `subject`. L'API mongodbee est en anglais. `personal` colle au texte de l'art. 4(1), `pii` est un terme américain qui n'existe pas dans le RGPD.
6. **Le décalage temporel global.** Le faire, ou accepter que les dates absolues soient un quasi-identifiant assumé dans un domaine restreint (EDPB §103 permet de garder des quasi-identifiants quand le domaine est petit et contrôlé).
