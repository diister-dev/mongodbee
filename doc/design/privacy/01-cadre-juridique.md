# Cadre juridique : ce que le droit demande à un outil de schéma

Statut : analyse de brainstorm. Textes lus le 2026-09-01. Les citations sont reproduites depuis les sources, pas de mémoire.

## Sources

| Source | Ce qu'on y a lu | Lien |
|---|---|---|
| RGPD, art. 4, 5, 9, 11, 17, 20, 25, 30, 32, 89 ; considérants 26 et 28 | Définitions et obligations | [gdpr-info.eu](https://gdpr-info.eu/) |
| EDPB, Guidelines 01/2025 on pseudonymisation, version adoptée pour consultation publique le 16 janvier 2025 | Transformation, information supplémentaire, domaine, quasi-identifiants, politiques de pseudonymes | [edpb.europa.eu](https://www.edpb.europa.eu/our-work-tools/documents/public-consultations/2025/guidelines-012025-pseudonymisation_en) |
| G29, avis 05/2014 sur les techniques d'anonymisation, via la CNIL | Les trois critères, les deux familles de techniques | [cnil.fr](https://www.cnil.fr/fr/le-g29-publie-un-avis-sur-les-techniques-danonymisation) |
| CNIL, « L'anonymisation de données personnelles » | Définitions, critères, réévaluation dans le temps | [cnil.fr](https://www.cnil.fr/fr/lanonymisation-de-donnees-personnelles) |
| CJUE, C-413/23 P, EDPS c/ SRB, arrêt du 4 septembre 2025 | Le caractère relationnel de la donnée pseudonymisée | [fpf.org](https://fpf.org/blog/rethinking-personal-data-the-cjeus-contextual-turn-in-edps-vs-srb/), [jonesday.com](https://www.jonesday.com/en/insights/2025/09/cjeu-clarifies-scope-of-personal-data-in-edps-v-srb-decision) |
| ENISA, Pseudonymisation techniques and best practices (2019) | Techniques et politiques, cité par l'EDPB au chapitre 5 | [enisa.europa.eu](https://www.enisa.europa.eu/publications/pseudonymisation-techniques-and-best-practices) |
| CNIL, Guide RGPD du développeur, fiche 11 « Tester vos applications » | Données réelles en développement | [github.com/LINCnil](https://github.com/LINCnil/Guide-RGPD-du-developpeur/blob/main/11-Tester%20vos%20applications.md) |
| Diivento, `projects/legal/rgpd/` | Registre art. 30, audit des écarts, questions ouvertes | dépôt diivento |

Réserve : la version EDPB lue est celle soumise à consultation. Une version finale a pu amender des paragraphes. Les numéros de paragraphes cités ci-dessous sont ceux de cette version.

## 1. Les définitions qui fixent le vocabulaire de l'outil

### 1.1 Donnée à caractère personnel, art. 4(1)

> « any information relating to an identified or identifiable natural person »

Ce que ça change pour l'outil : la question n'est pas « ce champ est-il un nom ou un email ». La question est « ce document se rapporte-t-il à une personne, et ce champ aide-t-il à la retrouver ». Un `createdAt` sur un document rattaché à une personne est une donnée personnelle. Une note de modérateur sur un utilisateur est une donnée personnelle de cet utilisateur. La classification doit donc porter d'abord sur **le sujet du document**, ensuite sur la nature de chaque champ. C'est l'axe `subject` du document 02.

### 1.2 Pseudonymisation, art. 4(5) et EDPB §18 à 22

> « processing of personal data in such a manner that the personal data can no longer be attributed to a specific data subject without the use of additional information »

L'EDPB découpe la définition en deux objets qui vont toujours ensemble :

- la **transformation pseudonymisante** (§18) : « a procedure that modifies original data in a way that the result [...] cannot be attributed to a specific data subject without additional information » ;
- l'**information supplémentaire** (§19, §20) : « information whose use enables the attribution of pseudonymised data to identified or identifiable persons ». Elle inclut « tables matching pseudonyms with the identifying attributes they replace. It may also consist of cryptographic keys ». La note 21 ajoute que **les données d'origine conservées sont elles aussi de l'information supplémentaire**.

Et la conséquence qui tranche le statut du dump de développement, §22 :

> « Pseudonymised data, which could be attributed to a natural person by the use of additional information, is to be considered information on an identifiable natural person, and is therefore personal. This statement also holds true if pseudonymised data and additional information are not in the hands of the same person. »

Ce que ça change pour l'outil :

- La clé HMAC, la table de correspondance et la base de production sont trois formes de la même chose : de l'information supplémentaire. L'outil doit traiter la clé comme un secret au sens fort, et la jeter ou la garder est une décision de politique, pas d'implémentation.
- Tant que la prod existe, l'extrait pseudonymisé **reste une donnée personnelle** pour Diivento. Le mot « anonymisé » ne doit pas apparaître dans l'API de l'outil pour désigner ce qu'il produit.

### 1.3 Anonymisation, considérant 26, G29 05/2014, CNIL

Considérant 26 :

> « The principles of data protection should therefore not apply to anonymous information, namely information which does not relate to an identified or identifiable natural person or to personal data rendered anonymous in such a manner that the data subject is not or no longer identifiable. »

Et le critère : « all the means reasonably likely to be used, such as singling out ».

Le G29 en tire trois critères que la CNIL reprend mot pour mot :

1. **Individualisation** : « il ne doit pas être possible d'isoler un individu dans le jeu de données ».
2. **Corrélation** : « il ne doit pas être possible de relier entre eux des ensembles de données distincts ».
3. **Inférence** : « il ne doit pas être possible de déduire, de façon quasi certaine, de nouvelles informations sur un individu ».

Et la phrase qui ferme la porte : « les données pseudonymisées ne sont pas des données anonymes ». Le hachage, le chiffrement et la tokenisation sont classés comme pseudonymisation, pas anonymisation.

Ce que ça change pour l'outil : la substitution champ par champ ne satisfait aucun des trois critères. Un extrait où chaque personne garde sa ligne, ses relations et ses attributs catégoriels reste individualisable et corrélable. **L'outil produit de la pseudonymisation. L'anonymisation est une propriété d'un jeu de données évaluée au cas par cas, pas une fonction qu'on appelle.** La CNIL demande en plus une réévaluation dans le temps, ce qu'aucun outil ne peut promettre.

### 1.4 Le tournant relationnel, CJUE EDPS c/ SRB, 4 septembre 2025

La Cour juge que des données pseudonymisées peuvent être personnelles pour le responsable qui détient l'information supplémentaire et ne pas l'être pour un destinataire qui n'a aucun moyen raisonnable de réidentifier. L'appréciation se fait du point de vue de chaque acteur, en tenant compte des facteurs techniques, organisationnels et juridiques.

Ce que ça change pour l'outil : la question « le poste de dev détient-il une donnée personnelle » dépend de ce que le poste de dev peut faire. S'il n'a ni la clé, ni accès à la prod, ni les moyens de recouper, la réponse penche vers non. Mais Diivento, qui a tout ça, reste responsable, et le périmètre où circule l'extrait doit être étanche. C'est exactement ce que l'EDPB appelle le **domaine de pseudonymisation** (§35 à 41) : l'ensemble des acteurs, systèmes et moyens dans lequel l'information supplémentaire ne doit pas entrer et la donnée pseudonymisée ne doit pas sortir.

### 1.5 Catégories particulières, art. 9(1)

> « racial or ethnic origin, political opinions, religious or philosophical beliefs, or trade union membership, and the processing of genetic data, biometric data for the purpose of uniquely identifying a natural person, data concerning health or data concerning a natural person's sex life or sexual orientation »

Ce que ça change pour l'outil : une classe `sensitive` distincte des autres, avec le traitement le plus fermé par défaut. Le registre Diivento constate déjà le manque (AUDIT-ECARTS écart 9) : aucun drapeau lisible par la machine sur les champs personnalisés. La CNIL exige en plus que les données sensibles soient traitées « avec parcimonie ».

## 2. Les obligations qui créent un besoin outillé

| Article | Ce qu'il dit | Capacité outil correspondante |
|---|---|---|
| 5(1)(c) minimisation | « adequate, relevant and limited to what is necessary » | Projection : ne sortir que les champs nécessaires à la finalité de l'extrait. |
| 5(1)(e) limitation de la conservation | « kept in a form which permits identification of data subjects for no longer than is necessary » | La pseudonymisation à échéance est une façon de satisfaire 5(1)(e) sans supprimer. C'est le choix de la politique de confidentialité Diivento (2 ans après l'événement). Le TTL `expireAfterSeconds` porté par `withIndex` est déjà une déclaration de durée dans le schéma. |
| 5(1)(f) et 32(1)(a) | La pseudonymisation et le chiffrement sont **la première mesure nommée** à l'art. 32 | La transformation elle-même. |
| 5(2) accountability | « be able to demonstrate compliance » | Le rapport de classification. Sans lui, on ne peut pas montrer ce qui a été transformé et ce qui ne l'a pas été. |
| 25(1) et (2) by design et by default | Pseudonymisation citée comme exemple de mesure ; « by default, only personal data which are necessary for each specific purpose » | Porter la classification dans le schéma, à côté du type et de l'index, c'est du by design au sens propre. |
| 17 effacement, et 17(3)(b) et (e) exceptions | Effacement sans retard, sauf obligation légale ou défense en justice | Le parcours par sujet doit connaître une classe « à conserver pour obligation légale » (facturation 10 ans, journaux de sécurité). |
| 20 portabilité | « structured, commonly used and machine-readable format » ; 20(4) sans nuire aux droits d'autrui | Une projection par sujet, filtrée sur ce que la personne a fourni, en excluant les champs qui se rapportent à d'autres (auteur d'un commentaire, scanneur). |
| 30(1)(c) et (f) registre | « categories of data subjects and of the categories of personal data » ; « envisaged time limits for erasure » | Les deux sont **dérivables du schéma** si la classification y est. Voir document 02, section 6. |
| 89(1) statistiques | Pseudonymisation comme garantie ; obligation de préférer un traitement qui ne permet plus l'identification quand la finalité le permet | Les analytics post-événement doivent tourner sur des données pseudonymisées, pas sur les identités. |
| 11 | Si le responsable ne peut plus identifier, il n'a pas à acquérir de quoi le faire, et les art. 15 à 20 ne s'appliquent plus | Argument pour **jeter la clé** après la pseudonymisation à échéance : l'art. 11 dit qu'on n'a pas à garder de quoi revenir en arrière. |

## 3. Ce que l'EDPB impose techniquement

Ces paragraphes sont ceux qui contraignent le design. Ils sont cités parce qu'ils valident ou invalident directement des choix d'implémentation.

**Ce qui doit être transformé.**

- §83 : « pseudonymised data must not contain direct identifiers [...] those identifiers are removed in the course of the pseudonymising transformation. Direct identifiers may, however, be replaced by new identifiers ».
- §84 : la transformation « also modifies other attributes, e.g. by removal, generalisation and noise addition ».
- §101 : les **quasi-identifiants** sont des combinaisons d'attributs suffisantes pour attribuer, avec pour exemples « age, gender, languages spoken, marital or family status, profession, income ». Pour des employés : « structural role, number of working hours, length of service ».
- §102 à 104 : trois réponses aux quasi-identifiants : suppression, généralisation ou randomisation, ou réduction du domaine. La troisième n'est pas disponible quand on protège contre des tiers externes.

**Le secret et sa garde.**

- §85 : les secrets sont « either cryptographic keys (for encryption or one-way functions) or tables matching pseudonyms with the personal data they replace ».
- §86 : ils font partie de l'information supplémentaire et doivent être gardés séparément.
- §88 et note 23 : un hachage sans clé est cassé par construction. « if you know the transformation is merely a SHA256 hash of a name, you could apply this to all names you have elsewhere and then see which hashes match ».
- §89 : « Preference should generally be given to one-way functions due to the difficulty of their reversal even when the secret parameters are known ». Le chiffrement seulement si la réversibilité est un besoin. HMAC est cité nommément (§107).
- Note 26 : quand l'entrée a peu d'entropie, utiliser une fonction conçue pour les mots de passe, argon2 est cité.
- §91 : prévoir un plan de remplacement des pseudonymes, idéalement en appliquant une seconde fonction aux anciens pseudonymes sans repasser par les données d'origine.
- §93 : les tables de correspondance sont elles-mêmes des données personnelles et doivent être protégées comme telles.

**Les politiques de cohérence, §115 à 121.** C'est le passage le plus directement utile au design.

- **Pseudonymes de personne** (§116) : le même pseudonyme partout, pour toutes les données d'une personne. Exige la conservation longue du secret. Risque d'attribution le plus élevé.
- **Pseudonymes de relation** (§117) : un pseudonyme par type de relation ou par partenaire. Secret gardé le temps de la relation.
- **Pseudonymes de transaction** (§119) : un pseudonyme différent par transaction. « contributes most effectively to data minimisation and data protection by default ».
- §121 : « the controller should define the sets of data that are to be pseudonymised consistently as small as possible ».

Ce que ça change pour l'outil : le moteur de corrélation de mongodbee clé déjà ses pools sur `(espace × scope)`. Dans Diivento, `scope` est l'exposition. **La clé de pool actuelle est un pseudonyme de relation par exposition.** Clé sur l'espace seul, c'est un pseudonyme de personne. Pas de pool, c'est un pseudonyme de transaction. Le choix EDPB est déjà un paramètre existant du moteur, il suffit de l'exposer et de le nommer.

**Le domaine.**

- §111 : « Appropriate measures should be in place to ensure additional information does not enter the pseudonymisation domain. Likewise [...] pseudonymised data does not leave it ».
- §112 : « Copies of data should be deleted as soon as they are no longer needed ».
- §105 : deux façons d'introduire la pseudonymisation : par un proxy dédié, ou à la source avant transmission. Un extrait produit côté prod avant transfert est une pseudonymisation à la source.

## 4. Le guide CNIL du développeur, fiche 11

> « Les données « réelles » de production ne doivent pas être utilisées pendant la phase de développement et de test. »

> « Utiliser les données personnelles issues de votre base de production à des fins de tests revient à les détourner de leur finalité initiale. »

> « Construisez donc un jeu de données fictives qui ressemblera aux données qui seront traitées par votre application. »

> « Si vous avez besoin d'importer des configurations existantes depuis la production dans vos scénarios de test, pensez à anonymiser les données personnelles qui peuvent être présentes. »

Ce que ça change pour l'outil : la CNIL nomme les deux directions, dans cet ordre. **Le jeu fictif ressemblant est la recommandation première**, l'import transformé est le secours. C'est l'ordre du README : le seeding d'abord, l'extrait ensuite. Et la CNIL cite Faker comme outil, ce que valibot-mock embarque déjà.

## 5. Ce que le droit ne demande pas à mongodbee

- **Décider ce qui est personnel.** C'est le responsable de traitement. mongodbee infère et propose, le consommateur confirme ou corrige, le rapport trace. Une inférence silencieusement acceptée serait un problème ; une inférence rapportée est une aide.
- **Le registre, les bases légales, les durées.** Ce sont des faits de politique. L'outil peut en dériver la partie qui est dans le schéma (catégories, durées techniques), pas le reste.
- **La garde des secrets.** HSM, partage de secret, rotation : l'outil expose une interface de source de clé, il n'en implémente pas la gouvernance. Diivento a déjà une abstraction `KekSource` pour cette forme.
- **L'anonymisation au sens du considérant 26.** k-anonymat, l-diversité, confidentialité différentielle sont des propriétés de jeux de données, à évaluer par un humain. L'outil ne doit pas porter un mot qu'il ne peut pas tenir.

## 6. Vocabulaire à tenir dans l'API et la documentation

| Mot | Sens tenu | Ne pas confondre avec |
|---|---|---|
| **fictif** | Généré ex nihilo depuis le schéma. Aucune personne réelle derrière. | pseudonymisé |
| **pseudonymisé** | Transformé depuis du réel, avec un secret. Reste personnel tant que le secret ou l'origine existe. | anonymisé |
| **anonymisé** | Propriété d'un jeu de données évaluée sur les trois critères. **Jamais le nom d'une commande.** | pseudonymisé |
| **chiffré** | Réversible avec la clé, valeur inutilisable sans. Pas une pseudonymisation au sens de l'outil : le champ garde son sens, il change de forme. | pseudonymisé |
| **minimisé** | Champs retirés parce que non nécessaires à la finalité. | pseudonymisé |
| **opaque** | Valeur qu'on ne sait ni générer ni transformer sans casser sa cohérence : texte chiffré, hachage, jeton. | technique |

## 7. Points repérés dans le registre Diivento en lisant

Ces points ne sont pas des questions d'outil, mais l'outil les rendrait visibles.

- **La politique de confidentialité promet une pseudonymisation à 2 ans.** La fonction en place, `anonymize.ts`, **supprime** cinq clés bien connues. Pas de pseudonyme, pas de secret. Au sens de l'EDPB c'est une suppression partielle, pas une pseudonymisation. Les champs personnalisés ne sont pas touchés, le fichier le dit lui-même. Le rapport de classification montrerait l'écart entre la promesse et le parcours réel.
- **ST-03 décrit `exhibitor_contact` comme portant une copie de l'identité du visiteur.** Le schéma actuel de `lead.schemas.ts` référence `participantId` et ne porte pas de bloc `identity`. Si c'est confirmé, la question ouverte 7.1 du registre (cascade de la pseudonymisation vers la fiche exposant) est en partie sans objet, et la fiche ST-03 est à jour à faire. C'est un exemple concret de dérive entre registre et schéma, celle-là même qu'une section de registre dérivée du schéma empêcherait.
- **L'écart 7 du registre (TTL partiel par `kind` sur `+emails`)** est bloqué côté mongodbee : impossible de déclarer plusieurs index TTL sur un même champ avec des filtres partiels différents. C'est un manque de la même famille que ce document : le schéma devrait pouvoir porter la durée de conservation par discriminant.
- **L'enveloppe chiffrée de `+emails`** (`ciphertext`, `dekRef`, `iv`, `authTag`) est le cas d'école de la classe opaque. La mémoire du chantier de corrélation garde la trace d'une migration qui a gelé `refId("dek")` contre des valeurs base64 réelles. Un transformateur naïf ferait la même erreur.
