Contrôles et transformations réalisées
======================================
Name | Description | Step | Type
-- | -- | -- | --
file_name_error | Le nom du fichier ne contient que des chiffres | upload | error
file_name_error | Le nom du fichier commence par un chiffre | upload | error
file_name_error | Le nom du fichier fait plus de 50 caractères | upload | error
no_data | Aucune données n'est présente dans le fichier | upload | error
extension_error | L'extenstion du fichier n'est pas .csv ou .geojson | upload | error
no_file | Aucun fichier détecté | upload | error
empty | Auncun fichier envyé | upload | error
max_size | La taille du fichier est plus grande que la taille définit en paramètre | upload | error
source-error(goodtables lib) | Erreur de lecture du fichier lié à une inconsistence de données. | csv | error
format-error(goodtables lib) | Erreur de lecture (format des données incorrect). | csv | error
encoding-error(goodtables lib) | Erreur de lecture (problème d'encodage). | csv | error
blank-header(goodtables lib) | Il existe une colonne vide dans les noms de colonnes. | csv | error
duplicate-header(goodtables lib) | Plusieurs colonnes ont le même nom | csv | error
blank-row(goodtables lib) | Une ligne doit avoir au moins une colonne non vide. | csv | error
duplicate-row(goodtables lib) | Ligne dupliquée. | csv | error
extra-value(goodtables lib) | Une ligne a plus de colonne que l'entête | csv | error
missing-value(goodtables lib) | Une ligne a moins de colonne que l'entête. | csv | error
wrong id_dataset | L'utilisateur n'as pas les droits d'importer dans ce JDD (à vérifier/implémenter ?) | load raw data to db | error
psycopg2.errors.BadCopyFileFormat | Erreur lié à un problème de séparateur | load row data to db | error
missing_value | Valeur manquante pour un champ obligatoire | data cleaning | error
incorrect_date | Format de date incorrect - Format attendu est YYYY-MM-DD ou DD-MM-YYYY (les heures sont acceptées sous ce format: HH:MM:SS)  | data cleaning | error
incorrect_uuid | Format d'UUID incorrect | data cleaning | error
incorrect_length | Chaine de charactère trop longue par rapport au champs de destination | data cleaning | error
incorrect_integer | Valeur incorect (ou négative) pour un champs de type entier | data cleaning | error
incorrect_cd_nom | Le cd_nom fourni n'est pas présent dans le taxref de l'instance | data cleaning | error
incorrect_cd_hab | Le cd_hab fourni n'est pas présent dans le habref de l'instance | data cleaning | error
date_min > date_max | date min > date_max | data cleaning | error
missing_uuid | UUID manquant (si calculer les UUID n'est pas coché) | data cleaning | warning
duplicated uuid | L'UUID fourni est déjà présent en base (dans la table synthese) - désactivable pour les instances avec beaucoup de données: paramètre `ENABLE_SYNTHESE_UUID_CHECK` | data cleaning | error
unique_id_sinp missing column | Si pas de colonne UUID fournie est que "calculer les UUID" est activé, on crée une colonne et on crée des UUID dedans | data cleaning | checks and corrects
unique_id_sinp missing values | Si UUID manquant dans une colonne UUID fournie et que "calculer les UUID" est activé, on calcul un UUID pour les valeurs manquantes | data cleaning | checks and corrects
missing count_min value | Si des valeurs sont manquantes pour denombrement_min, la valeur est remplacée par le paramètre DEFAULT_COUNT_VALUE | data cleaning | checks and corrects
missing count_max values | Si des valeurs sont manquantes pour denombrement_min, on met denombrement_min = denombrement_max | data cleaning | checks and correct
missing count_min column | Si pas de colonne dénombrement_min, on cree une colonne et on met la valeur du paramètre DEFAULT_COUNT_VALUE | data cleaning | checks and corrects
missing count_max column | Si pas de colonne denombrement_max on cree une colonne et on met denombrement_min = denombrement_max | data cleaning | checks and corrects
missing altitude_min and altitude_max columns | Creation de colonne et calcul si 'calcul des altitudes' est coché | data cleaning | checks and corrects
missing altitude_min or altitude_max values | Les altitdes sont calculées pour les valeurs manquantes si l'option est activée | data cleaning | checks and corrects
altitude_min > altitude_max | altitude_min > altitude_max  | data cleaning | checks and corrects
profondeur_min > profondeur_max | profondeur_min > profondeur_max  | data cleaning | checks and corrects
count_min > count_max | count_min > count_max | data cleaning | error
entity_source_pk column missing | Si pas de colonne fournie, création d'une colonne remplie avec un serial "gn_pk" | data cleaning | checks and corrects
entity_source_pk duplicated | entity_source_pk value dupliqué | data cleaning | error
incorrect_real | Valeur incorect pour un réel | data cleaning | error
geometry_out_of_box | Coordonnées géographiques en dehors de la bounding-box de l'instance (paramètre: `INSTANCE_BOUNDING_BOX` - desactivable via paramètre `ENABLE_BOUNDING_BOX_CHECK` | data cleaning | error
geometry_out_of_projection | Coordonnées géographiques en dehors du système de projection fourni | data cleaning | error
multiple_code_attachment | Plusieurs code de rattachement fourni pour une seule colonne (Ex code_commune = 05005, 05003) | data cleaning | error
multiple_attachment_type_code | Plusieurs code de rattachement fourni pour une seule ligne (code commune + code maille par ex) | data cleaning | error
code rattachement invalid | Le code de rattachement (code maille/département/commune) n'est pas dans le référentiel géographiques de GeoNature | data cleaning | error
Erreur de nomenclature | Code nomenclature erroné ; La valeur du champ n’est pas dans la liste des codes attendus pour ce champ. | data cleaning | error
Erreur de géometrie | Géométrie invalide ; la valeur de la géométrie ne correspond pas au format WKT. | data cleaning | error
Géoréférencement manquant | Géoréférencement manquant ; un géoréférencement doit être fourni, c’est à dire qu’il faut livrer : soit une géométrie, soit une ou plusieurs commune(s), ou département(s), ou maille(s) | data cleaning | error
Preuve numérique incorect | La preuve numérique fournie n'est pas une URL | data cleaning | error
Erreur champs conditionnel (désactivable)| Le champ dEEFloutage doit être remplit si le jeu de données est privé | data cleaning | error
Erreur champs conditionnel (désactivable) | Le champ reference_biblio doit être remplit si le statut source est 'Littérature' | data cleaning | error
Erreur champs preuve (désactivable) | si le champ “preuveExistante” vaut oui, alors l’un des deux champs “preuveNumérique” ou “preuveNonNumérique” doit être rempli. A l’inverse, si l’un de ces deux champs est rempli, alors “preuveExistante” ne doit pas prendre une autre valeur que “oui” (code 1) | data cleaning | error
Erreur de rattachement(1) - désactivable | Vérifie que si type_info_geo = 1 (Géoréférencement) alors aucun rattachement n'est fourni | data cleaning | error
Erreur de rattachement(2) - désactivable |  Si une entitié de rattachement est fourni alors le type_info_geo ne doit pas être null | data cleaning | error

