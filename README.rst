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
- Certains mappings sont définit comme "public" et sont vu par tout le monde. Seul les administrateurs (U=3) et les propriétaires de ces mappings peuvent les modifier.
- Avec un C = 1,  R = 3, U = 1  un utilisateur pourra par exemple créer des nouveaux mappings (des modèles d'imports), modifier ses propres mappings et voir l'ensemble des mappings de l'instance. 
- Si vous modifiez un mapping sur lequel vous n'avez pas les droits, un mapping temporaire sera créé dans la BDD en enregistrant les modifications que vous avez faites, mais sans modifier le mapping initial. Lorsqu'on a les droits de modification sur un mapping, il est également possible de ne pas enregistrer les modifications faites à celui-ci pour ne pas écraser le mapping inital (un mapping temporaire sera également créé pour votre import en cours).
- Si vous voulez conserver des mappings "types" que personne ne pourra modifier sauf les administrateurs, mettez le CRUVED suivant sur l'objet mapping à votre groupe d'utilisateur "non administrateur" : C = 1,  R = 3, U = 1, D = 1


Contrôles et transformations
============================

`Liste des contrôles <docs/controls.rst>`_

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
   pip install -r /home/`whoami`/gn_module_import/backend/requirements.txt
   geonature update_module_configuration IMPORT


Utilisation du module d'imports
===============================

Le module permet de traiter un fichier CSV ou GeoJSON sous toute structure de données, d'établir les correspondances nécessaires entre le format source et le format de la synthèse, et de traduire le vocabulaire source vers les nomenclatures SINP. Il stocke et archive les données sources et intègre les données transformées dans la synthèse de GeoNature. Il semble préférable de prévoir un serveur disposant à minima de 4 Go de RAM. 

1. Une fois connecté à GeoNature, accédez au module Imports. L'accueil du module affiche une liste des imports en cours ou terminés, selon les droits de l'utilisateur connecté. Vous pouvez alors finir un import en cours, ou bien commencer un nouvel import. 

.. image:: https://geonature.fr/docs/img/import/gn_imports-01.jpg

2. Choisissez à quel JDD les données importées vont être associées. Si vous souhaitez les associer à un nouveau JDD, il faut l'avoir créé au préalable dans le module métadonnées.

.. image:: https://geonature.fr/docs/img/import/gn_imports-02.jpg

3. Chargez le fichier CSV ou GeoJSON à importer. Le nom du fichier ne doit pas dépasser 50 caractères.

.. image:: https://geonature.fr/docs/img/import/gn_imports-03.jpg

4. Mapping des champs. Il s'agit de faire correspondre les champs du fichier importé aux champs de la Synthèse (basé sur le standard "Occurrences de taxons" du SINP). Vous pouvez utiliser un mapping déjà existant ou en créer un nouveau. Le module contient par défaut un mapping correspondant à un fichier exporté au format par défaut de la synthèse de GeoNature. Si vous créez un nouveau mapping, il sera ensuite réutilisable pour les imports suivants. Il est aussi possible de choisir si les UUID uniques doivent être générés et si les altitudes doivent être calculées automatiquement si elles ne sont pas renseignées dans le fichier importé. Il est également possible de sélectionner un ou plusieurs champs sources "personnalisés" pour alimenter les attributs additionnels dans la synthèse.

.. image:: https://geonature.fr/docs/img/import/gn_imports-04.jpg

6. Mapping des contenus. Il s'agit de faire correspondre les valeurs des champs du fichier importé avec les valeurs disponibles dans les champs de la Synthèse de GeoNature (basés par défaut sur les nomenclatures du SINP). Par défaut les correspondances avec les nomenclatures du SINP sous forme de code ou de libellés sont fournies. Si les champs sélectionnés lors de l'étape précédente ne correspondent à aucun champ de nomenclature, alors cette étape est automatiquement passée.

.. image:: https://geonature.fr/docs/img/import/gn_imports-06.jpg

7. Une fois l'ensemble des correspondances réalisées (champs et contenu) - au moins sur les champs obligatoires - il faut alors valider le mapping pour lancer le contrôle des données. Vous pourrez ensuite consulter les éventuelles erreurs lors de l'étape de prévisualisation. Il est éventuellement possible de corriger les données en erreurs directement dans la base de données, dans la table des données en cours d'import, puis de revalider le mapping, ou de passer à l'étape suivante. Les données en erreur ne seront pas importées et seront téléchargeables dans un fichier dédié à l'issue du processus.

.. image:: https://geonature.fr/docs/img/import/gn_imports-05.jpg

8. La dernière étape permet d'avoir un aperçu des données à importer et leur nombre, et de consulter le détail des erreurs identifiées lors des contrôles avant de valider l'import final dans la Synthèse de GeoNature. Vous pouvez également taguer un import nécessitant des corrections ou compléments ultérieurs, et y associer un commentaire pour revenir sur cet import ou le compléter par la suite.

.. image:: https://geonature.fr/docs/img/import/gn_imports-07.jpg

10. Depuis la liste des imports, vous pourrez consulter un rapport d'import fournissant une description des données importées, le détail des erreurs rencontrées, permettant également de télécharger les données invalides ainsi que l'ensemble des correspondances effectuées au format JSON en vue de partager les mappings entre plusieurs instances de GeoNature. Le rapport d'import PDF est exportable au format PDF pour en faciliter la diffusion auprès de partenaires fournisseurs de données notamment. 

Pour chaque fichier importé, les données brutes sont importées et stockées tel quel dans une table portant le nom du fichier, dans le schéma ``gn_import_archives``. Elles sont aussi stockées dans une table intermédiaire, enrichie au fur et à mesure des étapes de l'import pour transformer les données vers les formats du SINP.

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

3. La table créée dans le schéma ``gn_imports`` est la table de travail sur laquelle les différentes transformations et différents compléments seront effectués au cours du processus. Cette table se voit dotée de 2 champs "techniques" : ``gn_invalid_reason`` (ensemble des erreurs détectées rendant la donnée invalide), et ``gn_pk`` (clé primaire purement technique).

A la fin du processus, seules les données valides (dont le champs ``gn_invalid_reason`` est vide) seront importées dans la synthèse. 

4. Entre les différents mappings et à l'issue de l'étape 3 (mapping de contenus), des modifications peuvent être effectuées sur la table de travail, directement dans la base de données. 

Le module permet ainsi l'ajout de nouveaux champs (ajout et calcul d'un champs cd_nom par l'administrateur par exemple), ou le travail sur les données en cours d'import (rentre invalides des données n'appartenant pas à un territoire etc). Le module, initialement conçu comme un outil d'aide à l'import des données pour les administrateurs, permet donc de modifier, corriger, ou travailler sur les données dans la base au cours du processus.  

Financement de la version 1.0.0 : DREAL et Conseil Régional Auvergne-Rhône-Alpes dans le cadre de l'animation régionale du SINP via le Pôle Invertébrés.
