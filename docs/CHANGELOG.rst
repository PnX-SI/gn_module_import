=========
CHANGELOG
=========

0.2.0 (unreleased)
------------------

**Corrections**

* Contrôle des doublons sur les uuid fournis dans les données sources
* Prise en charge des uuid sources lorsqu'ils sont importés en majuscule
* Correction de la récupération des jeux de données en fonction de l'utilisateur
* Correction du calcul du nombre de taxons importés (basé sur le cd_ref et non plus sur le cd_nom)
* Masquage des champs obligatoires rendu impossible dans la configuration de l'interface
* Ajout de la constante 'I' (Import) dans le champ "last action" de la synthèse lors de l'import
* Correction du chemin du répertoire upload
* Nom du module repassé en variable lors de l'installation


**Améliorations**

* Précision au survol sur l'icone de téléchargement des données invalides (étape 1)

0.1.0 (2019-12-19)
------------------

Première version fonctionelle du module Import de GeoNature

**Fonctionnalités**

* Création d'un schéma ``gn_imports`` incluant les tables des imports, des mappings, des messages d'erreurs et des champs de destination des imports
* Liste des imports terminés ou en cours en fonction des droits de l'utilisateur
* Création de nouveaux imports et upload de fichiers CSV ou GeoJSON
* Création d'une table des données brutes pour chaque import
* Contrôle automatique des fichiers (#17)
* Mapping des champs puis des valeurs des champs, définis dans 2 tables listant les champs de destination
* Création d'une table des données enrichies pour chaque import
* Possibilité de corriger, mettre à jour ou compléter la table enrichie en cours de processus
* Enregistrement des mappings pour pouvoir les réutiliser pour un autre import
* Contrôle des erreurs et téléchargement des données erronées
* Flexibilité de l'interface et des regroupements de champs, paramétrable via les tables ``gn_import.dict_themes`` et ``gn_import.dict_fields``
* Import des données dans la synthèse
