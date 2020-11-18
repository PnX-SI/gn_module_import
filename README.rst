Installation du module d'imports
================================

Télécharger puis renommer la version souhaitée du module :

::

   cd
   wget https://github.com/PnX-SI/gn_module_import/archive/X.Y.Z.zip
   unzip X.Y.Z.zip
   rm X.Y.Z.zip
   mv gn_module_import-X.Y.Z gn_module_import


Le module doit ensuite être installé comme suit :

::

   cd
   source geonature/backend/venv/bin/activate
   geonature install_gn_module /home/`whoami`/gn_module_import import
   deactivate
   
Le module est installé et prêt à importer !

Configuration du module
=======================

La configuration du module se fait pour partie via le fichier ``conf_gn_module.toml``. Voir le fichier ``conf_gn_module.toml.example`` pour voir la liste des paramètres disponibles (champs affichés en interface à l'étape 1, préfixe des champs ajoutés par le module, répertoire d'upload des fichiers, SRID, encodage, séparateurs, etc). 

Une autre partie se fait directement dans la base de données, dans les tables ``dict_fields`` et ``dict_themes``, permettant de masquer, ajouter, ou rendre obligatoire certains champs à renseigner pour l'import. Un champs masqué sera traité comme un champs non rempli, et se verra associer des valeurs par défaut ou une information vide. Il est également possible de paramétrer l'ordonnancement des champs (ordre, regroupements dans des blocs) dans l'interface du mapping de champs. A l'instar des attributs gérés dans TaxHub, il est possible de définir des "blocs" dans la table ``gn_imports.dict_themes``, et d'y attribuer des champs (``dict_fields``) en y définissant leur ordre d'affichage.  

Après avoir regroupé les champs dans leurs "blocs" et leur avoir associé un ordre, vous devrez relancer le build de l'interface. 

::

   cd
   cd geonature/frontend
   npm run build

Droits du module
================

La gestions des droits dans le module d'import se fait via le réglage du CRUVED à deux niveaux : au niveau du module d'import lui-même et au niveau de l'objet "mapping".

- Le CRUVED du module d'import permet uniquement de gérer l'affichage des imports. Une personne ayant un R = 3 verra tous les imports de la plateforme, un R = 2 seulement ceux de son organisme et un R = 1 seulement les siens. 
- Les jeux de données selectionnables par un utilisateur lors de la création d'un import sont eux controlés par les permissions globales de GeoNature (et non du module d'import).
- Les mappings constituent un "objet" du module d'import disposant de droits paramétrables pour les différents rôles. Par défaut, les droits accordés sur les mappings sont identiques aux droits que les utilisateurs ont sur le module Import en lui-même. Le réglage des droits se fait dans le module "Admin" de GeoNature ("Admin" -> "Permissions"). 
- Avec un C = 1,  R = 3, U = 1  un utilisateur pourra par exemple créer des nouveaux mappings (des modèles d'imports), modifier ses propres mappings et voir l'ensemble des mappings de l'instance. 
- Si vous modifiez un mapping sur lequel vous n'avez pas les droits, un mapping temporaire sera créé dans la BDD en enregistrant les modifications que vous avez faites, mais sans modifier le mapping initial. Lorsqu'on a les droits de modification sur un mapping, il est également possible de ne pas enregistrer les modifications faites à celui-ci pour ne pas écraser le mapping inital (un mapping temporaire sera également créé pour votre import en cours).
- Si vous voulez conserver des mappings "types" que personne ne pourra modifier sauf les administrateurs, mettez le CRUVED suivant sur l'objet mapping à votre groupe d'utilisateur "non administrateur" : C = 1,  R = 3, U = 1, D = 1


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
Erreur champs conditionnel | Le champ dEEFloutage doit être remplit si le jeu de données est privé | data cleaning | error
Erreur champs conditionnel | Le champ reference_biblio doit être remplit si le statut source est 'Littérature' | data cleaning | error
Erreur champs preuve | si le champ “preuveExistante” vaut oui, alors l’un des deux champs “preuveNumérique” ou “preuveNonNumérique” doit être rempli. A l’inverse, si l’un de ces deux champs est rempli, alors “preuveExistante” ne doit pas prendre une autre valeur que “oui” (code 1) | data cleaning | error



Documentation developpeur
=========================

Procesus de vérification:

- le fichier fourni par l'utilisateur est converti en deux tables postgreSQL: un table d'archives et une table de travail sur laquelle seront faites les modifications et contrôles
- Pour les vérification, le fichier est transformé en dataframe Dask et traité avec la librairie Panda. Une fois les modifications et contrôles réalisés, le dataframe est rechargé dans la table de transformation (qui est au préalablement supprimée)
- Pour chaque champs de nomenclature, une colonne est créée pour transformer les valeurs fournies en id_nomenclature GeoNature. La colonne fournie se nomme "tr_<id_nomenclature_destination>_<nom_colonne_source>"
- la dernière étape de "prévisualisation" affiche l'ensemble des champs transformé tels qu'ils seront inserés dans GeoNature. Les nomenclatures sont affichées sous leur format décodé (label_fr), sous la forme "<champs_source>-><champ_destination>"

Mise à jour du module
=====================

- Téléchargez la nouvelle version du module

::

   wget https://github.com/PnX-SI/gn_module_import/archive/X.Y.Z.zip
   unzip X.Y.Z.zip
   rm X.Y.Z.zip


- Renommez l'ancien et le nouveau répertoire

::

   mv /home/`whoami`/gn_module_import /home/`whoami`/gn_module_import_old
   mv /home/`whoami`/gn_module_import-X.Y.Z /home/`whoami`/gn_module_import


- Rapatriez le fichier de configuration

::

   cp /home/`whoami`/gn_module_import_old/config/conf_gn_module.toml  /home/`whoami`/gn_module_import/config/conf_gn_module.toml


- Relancez la compilation en mettant à jour la configuration

::

   cd /home/`whoami`/geonature/backend
   source venv/bin/activate
   geonature update_module_configuration IMPORT


Utilisation du module d'imports
===============================

Le module permet de traiter un fichier CSV ou GeoJSON sous toute structure de données, d'établir les correspondances nécessaires entre le format source et le format de la synthèse, et de traduire le vocabulaire source vers les nomenclatures SINP. Il stocke et archive les données sources et intègre les données transformées dans la synthèse de GeoNature. Il semble préférable de prévoir un serveur disposant à minima de 4 Go de RAM. 

1. Une fois connecté à GeoNature, accédez au module Imports. L'accueil du module affiche une liste des imports en cours ou terminés, selon les droits de l'utilisateur connecté. Vous pouvez alors finir un import en cours, ou bien commencer un nouvel import. 

.. image:: https://geonature.fr/docs/img/import/gn_imports-01.jpg

2. Choisissez à quel JDD les données importées vont être associées. Si vous souhaitez les associer à un nouveau JDD, il faut l'avoir créé au préalable dans le module métadonnées.

.. image:: https://geonature.fr/docs/img/import/gn_imports-02.jpg

3. Chargez le fichier CSV ou GeoJSON à importer. Le nom du fichier ne doit pas dépasser 50 charactères.

.. image:: https://geonature.fr/docs/img/import/gn_imports-03.jpg

4. Mapping des champs. Il s'agit de faire correspondre les champs du fichier importé aux champs de la Synthèse (basé sur le standard "Occurrences de taxons" du SINP). Vous pouvez utiliser un mapping déjà existant ou en créer un nouveau. Le module contient par défaut un mapping correspondant à un fichier exporté au format par défaut de la synthèse de GeoNature. Si vous créez un nouveau mapping, il sera ensuite réutilisable pour les imports suivants. Il est aussi possible de choisir si les UUID uniques doivent être générés et si les altitudes doivent être calculées automatiquement si elles ne sont pas renseignées dans le fichier importé.

.. image:: https://geonature.fr/docs/img/import/gn_imports-04.jpg

6. Une fois le mapping des champs réalisé, au moins sur les champs obligatoires, il faut alors valider le mapping pour lancer le contrôle des données. Vous pouvez ensuite consulter les éventuelles erreurs. Il est alors possible de corriger les données en erreurs directement dans la base de données, dans la table temporaire des données en cours d'import, puis de revalider le mapping, ou de passer à l'étape suivante. Les données en erreur ne seront pas importées et seront téléchargeables dans un fichier dédié à l'issue du processus.

.. image:: https://geonature.fr/docs/img/import/gn_imports-05.jpg

7. Mapping des contenus. Il s'agit de faire correspondre les valeurs des champs du fichier importé avec les valeurs disponibles dans les champs de la Synthèse de GeoNature (basés par défaut sur les nomenclatures du SINP). Par défaut les correspondances avec les nomenclatures du SINP sous forme de code ou de libellés sont fournies.

.. image:: https://geonature.fr/docs/img/import/gn_imports-06.jpg

8. La dernière étape permet d'avoir un aperçu des données à importer et leur nombre, avant de valider l'import final dans la Synthèse de GeoNature.

.. image:: https://geonature.fr/docs/img/import/gn_imports-07.jpg

Pour chaque fichier importé, les données brutes sont importées intialement et stockées tel quel dans une table portant le nom du fichier, dans le schéma ``gn_import_archives``. Elles sont aussi stockées dans une table intermédiaire, enrichie au fur et à mesure des étapes de l'import.

Liste des contrôles réalisés sur le fichier importé et ses données : https://github.com/PnX-SI/gn_module_import/issues/17

Schéma (initial et théorique) des étapes de fonctionnement du module : 

.. image:: https://geonature.fr/docs/img/import/gn_imports_etapes.png

Modèle de données du schéma ``gn_imports`` du module :

.. image:: https://geonature.fr/docs/img/import/gn_imports_MCD-2020-03.png


Fonctionnement du module (serveur et BDD)
=========================================

1. Lors de la phase d'upload, le fichier source est chargé sur le serveur au format CSV ou GeoJson dans le répertoire "upload" du module. Le fichier en sera supprimé suite au processus afin de limiter l'espace occupé sur le serveur.

2. Suite à l'upload, les fichiers GeoJson sont convertis en CSV. Le CSV source ou le fichier converti en CSV est alors copié deux fois dans la base de données : 

- une fois dans le schéma ``gn_imports_archives`` : cette archive ne sera jamais modifiée, et permettra de garder une trace des données brutes telles qu'elles ont été transmises
- une fois dans le schéma ``gn_imports`` : cette copie est la table d'imports

3. La table créée dans le schéma ``gn_imports`` est la table de travail sur laquelle les différentes transformations et différents compléments seront effectués au cours du processus. Cette table se voit dotée de 3 champs "techniques" : ``gn_is_valid`` (booléen qui précise la validité de la ligne lors du processus d'import), ``gn_invalid_reason`` (ensemble des erreurs détectées rendant la donnée invalide), et ``gn_pk`` (clé primaire purement technique).

A la fin du processus, seules les données ``gn_is_valid=true`` seront importées dans la synthèse. 

4. Entre les différents mappings et à l'issue de l'étape 3 (mapping de contenus), des modifications peuvent être effectuées sur la table de travail, directement dans la base de données. 

Le module permet ainsi l'ajout de nouveaux champs (ajout et calcul d'un champs cd_nom par l'administrateur par exemple), ou le travail sur les données en cours d'import (rentre invalides des données n'appartenant pas à un territoire etc). Le module, initialement conçu comme un outil d'aide à l'import des données pour les administrateurs, permet donc de modifier, corriger, ou travailler sur les données dans la base au cours du processus.  

Financement de la version 1.0.0 : DREAL et Conseil Régional Auvergne-Rhône-Alpes.
