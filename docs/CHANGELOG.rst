=========
CHANGELOG
=========

2.3.0 (2024-02-28)
------------------

Nécessite la version 2.14.0 (ou plus) de GeoNature.

**🚀 Nouveautés**

 - Mise à jour vers SQLAlchemy 1.4 (#498)

2.2.3 (2023-09-28)
------------------

Nécessite la version 2.13.2 (ou plus) de GeoNature.

**🚀 Nouveautés**

* Amélioration des performances de la vérification des cd_nom des observations importées (#424)
* Amélioration des performances du chargement des données pour leur contrôle (#484)
* Amélioration des performances de l'analyse des colonnes du fichier source (#486)
* Amélioration des tests automatisés

**🐛 Corrections**

* Correction des permissions de la liste des JDD auxquels un utilisateur peut associer un import (#481)
* Correction du bouton d'import dans un JDD depuis sa fiche dans le module Métadonnées (#483)


2.2.2 (2023-09-19)
------------------

**🐛 Corrections**

* Ajout de la permissions disponible manquante "Modifier les imports" (#477)

**⚠️ Notes de version**

* Si vous aviez installé la version 2.2.0 ou 2.2.1, la permission disponible manquante "Modifier les imports" sera ajoutée automatiquement.
  Cependant, il est possible que vous deviez l'associer manuellement aux utilisateurs qui en disposaient auparavant.


2.2.1 (2023-09-13)
------------------

**🚀 Nouveautés**

* Prise en charge des virgules comme séparateur des décimales dans les coordonnées des champs X et Y (#473, par @bouttier)

**🐛 Corrections**

* Correction du rapport d'erreur quand des lignes contiennent des sauts de ligne (#464, par @cen-cgeier et @bouttier)
* Correction de la migration de suppression du schéma ``gn_import_archives`` (#471, par @joelclems)


2.2.0 (2023-08-23)
------------------

Nécessite la version 2.13.0 (ou plus) de GeoNature.

**🚀 Nouveautés**

* Compatibilité avec GeoNature 2.13.0 et la refonte des permissions, en définissant les permissions disponibles du module (#451)
* Vérification des permissions sur le nouvel objet "IMPORT" et non plus sur le module lui-même
* Ré-implémentation de la possibilité d'alimenter le champs ``additionnal_data`` de la synthèse avec un ou plusieurs champs du fichier source, ou depuis un champs JSON agrégeant déjà plusieurs informations (#165)
* Ré-implémentation du contrôle de validité des géométries des observations à importer (#453)
* Amélioration du contrôle des altitudes et des profondeurs (#445)
* Amélioration du rapport d'import en n'affichant le bloc "Données importées" seulement si l'import a un statut terminé (#457)
* Amélioration du tri dans la liste des imports en permettant de trier sur les colonnes avec une clé étrangère comme celle des JDD et des auteurs (#460)
* Amélioration des performances (x10) du contrôle d'intersection des géométries des observations à importer avec le zonage optionnellement défini dans le paramètre ``ID_AREA_RESTRICTION`` (#423)
* Compatibilité avec Debian 12 et Python 3.11, en adaptant la version de la dépendance "pyproj" selon la version de Python (#452)

**🐛 Corrections**

* Correction du lien de téléchargement du fichier source depuis la liste des imports (#456)
* Correction de la génération des UUID quand le champs permettant de les générer ou non est masqué et que le paramètre ``DEFAULT_GENERATE_MISSING_UUID`` est à ``True`` (#447)


2.1.0 (2023-03-27)
------------------

Nécessite la version 2.12.0 ou plus de GeoNature.

**🚀 Nouveautés**

* Compatibilité GeoNature 2.12 : Angular 15, configuration dynamique, configuration centralisée
* Ajout de notifications lorsqu'un import se termine, accessibles dans l'application ou envoyées par email (#414)
* Par défaut les utilisateurs sont abonnés à ces notifications et peuvent s'y désabonner (#440)
* Instrumentation avec Sentry des opérations coûteuses (chargement des données, contrôles, import en synthèse)
* Optimisation de la suppression des erreurs et des données transitoires lors de la reprise d’un import existant (#434)
* Compatibilité avec SQLAlchemy version 1.4
* Suppression du fichier ``config/settings.ini`` inutilisé

**🐛 Corrections**

* Correction de l'export CSV des données invalides (#433)
* Suppression de paramètres de configuration obsolètes depuis la version 2 :

  * ``UPLOAD_DIRECTORY``
  * ``IMPORTS_SCHEMA_NAME``
  * ``MISSING_VALUES``
  * ``INVALID_CSV_NAME``
  * ``DISPLAY_CHECK_BOX_MAPPED_VALUES``
  * ``MAX_LINE_LIMIT``
  * ``DISPLAY_CHECK_BOX_MAPPED_VALUES``
  * ``ALLOW_MODIFY_DEFAULT_MAPPING``
  * ``DISPLAY_MAPPED_FIELD``
  * ``CHECK_TYPE_INFO_GEO``


2.0.6 (2023-02-27)
------------------

**🐛 Corrections**

* Correction de la vérification des dénombrements afin d’effecter une comparaison entière et non textuelle (#343)
* Correction d’un bug lors de la mise-à-jour d’un mapping de valeurs lorsqu’un nouveau type de nomenclature est ajouté (#432)


2.0.5 (2023-02-14)
------------------

**🚀 Nouveautés**

* Ajout d’une limite temporelle sur le temps consacré à la détection de l’encodage afin d’éviter l’expiration de la requête lors du téléversement du fichier.
  La limite est définie par défaut à 2 secondes et modifiable via le paramètre ``MAX_ENCODING_DETECTION_DURATION`` (#422)


2.0.4 (2023-01-16)
------------------

**🚀 Nouveautés**

* Mise à jour de la documentation d’installation en accord avec les évolutions de GeoNature 2.11
* Possibilité de télécharger le fichier source (#416)
* Passage du paramètre ``FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE`` à ``true`` par défaut (#68)

**🐛 Corrections**

* Suppression d’une redirection inutile dûe à un slash final en trop dans une route
* Correction d’une migration v1 → v2 pour gérer les mappings vides
* Utilisation des nomenclatures par défaut de la Synthèse pour les champs de nomenclature non associés à l’étape de
  correspondance des champs. Pour les champs associés mais contenant des lignes vides, est utilisé
  la valeur précisée lors de la correspondance des champs, et à défaut, la nomenclature par défaut (#68)
  

2.0.3 (2022-12-14)
------------------

Nécessite la version 2.10.4 ou plus de GeoNature.

**🚀 Nouveautés**

* Refonte graphique du rapport d'import (#403)
* Affichage d’un bouton « Importer des données » dans le module métadonnées
* Ajout du contrôle que les altitudes et profondeurs ne soient pas négatives

**🐛 Corrections**

* Correction du processus de migration pour les imports créés avec la version 1.2 du module
* Pré-supposition de l’UTF-8 en cas d’impossibilité d’auto-détecter l’encodage
* Correction de la gestion des fichiers contenant des caractères UTF-8 dans leur nom
* Correction du format de fichier supporté en ne proposant que le CSV par défaut (seul format supporté actuellement)
* Correction d’une mauvaise remontée de l’état d’avancement du contrôle des imports


2.0.2 (2022-11-09)
------------------

**🐛 Corrections**

* Correction de la gestion des dates optionnelles non fournies
* Correction d’une erreur du frontend dans le rapport d’import


2.0.1 (2022-11-08)
------------------

**🐛 Corrections**

* Ajout de ``leaflet-image`` aux dépendances du frontend
* Correction de l’inversion du X et du Y lors de l’import
* Correction de l’inversion de la source et de la destination des correspondances de champs dans le rapport d’import
* Correction du processus de migration depuis la v1


2.0.0 (2022-11-02)
------------------

Nécessite la version 2.10.0 (ou plus) de GeoNature.
Refonte technique complète du module, améliorant son socle, sa robustesse et ses performances, et corrigeant de nombreux bugs [https://github.com/PnX-SI/gn_module_import/issues/257]

**🚀 Évolutions fonctionnelles [https://github.com/PnX-SI/gn_module_import/issues/257]**

* Pagination de la liste des imports côté serveur pour optimiser son chargement et affichage quand on a de nombreux imports
* Vérification des permissions sur le JDD.
* Découpage de l’étape de téléversement et paramétrages en 2 étapes distincts :

  * Téléversement du fichier
  * Sélection des paramètres du fichier :
    
    * Format : CSV uniquement (le support du GeoJSON est à rétablir)
    * Encodage : une liste configurable d’encodage est proposé avec l’encodage auto-détecté pré-sélectionné [https://github.com/PnX-SI/gn_module_import/issues/188]
    * Séparateur : une liste configurable de séparateur est proposé avec le séparateur auto-détecté pré-sélectionné
    * SRID (pas d’évolution)

* Le formulaire de correspondances des nomenclatures a été inversé : pour chaque nomenclature associée lors de la correspondance des champs sont affichées les valeurs source présente dans le fichier, avec un select permettant de choisir la nomenclature de destination. Il reste possible d’associer plusieurs champs source à une même nomenclature de destination, et ce sans multi-select.
* Gestion des modèles dans l’interface d’administration de GeoNature, 
* Possibilité de reprendre un import à n’importe quelle étape, y compris lorsque celui-ci est terminé (permettant de mettre à jour des données déjà importées dans la synthèse).
* Contrôle et import des données effectuées en asynchrone, peu importe le nombre de lignes du fichier.
* La dernière étape est dynamique, et affiche, suivant l’état de l’import :

  * Un bouton de lancement des contrôles;
  * Une barre de progression des contrôles;
  * La prévisualisation des données contrôlées et le bouton de lancement de l’import;
  * Un spinner d’attente pendant l’import;
  * Un rapport d’import.

* Suppression du TYP_INFO_GEO [https://github.com/PnX-SI/gn_module_import/issues/271]
* Utilisation des codes mailles longs [https://github.com/PnX-SI/gn_module_import/issues/218]

**💻 Évolutions techniques [https://github.com/PnX-SI/gn_module_import/issues/257]**

* Compatibilité avec Angular version 12, mis à jour dans la version 2.10.0 de GeoNature
* Packaging du module 
* Gestion de la BDD du module avec Alembic
* Suppression du code SQL au profit de l’utilisation de l’ORM
* Suppression des try/expect génériques ; les imports ne passent plus en erreur, mais l’erreur est collectée dans les logs de GeoNature et dans sentry et il est permis à l’utilisateur de réessayer en reprenant là où il en était
* Nombreuses corrections de bugs par l’écriture de code plus robuste
* Ajout de tests unitaires (couverture de code à 91%)
* Refonte des modèles d’imports :

  * Gestion correcte des permissions, ajout, modification, suppression
  * Les correspondances sont sauvegardées directement dans l’import indépendamment du modèle, résolvant ainsi les soucis liés à la reprise d’un import dont le modèle utilisé a été modifié, et supprimant le recours aux modèles temporaires
  * Les correspondances de champs / de nomenclatures sont stoquées au format JSON. Le format permet d’associer plusieurs valeurs sources à une même nomenclature de destination
   
* Asynchrone : utilisation d’un worker Celery permettant d’exécuter un seul contrôle / import à la fois (évite l’effondrement du serveur lors de plusieurs imports)
* Isolation du code de contrôle permettant de le tester automatiquement
* Factorisation de la gestion des erreurs
* Stockage du fichier source au format binaire dans une colonne de l’import. Cela rend inutile les tables d’archives qui sont supprimées ; les données sont préalablement migrées au format binaire.
* Suppression des tables transitoires créées à partir de la structure des fichiers CSV au profit d’une unique table transitoire. Les données sont chargées depuis le fichier source après l’étape de correspondance des champs
* La table transitoire contient un jeu de colonnes source et un jeu de colonnes destination ; les transformations sont refondues sur cette base, apportant un gain de simplification et de robustesse
* Les contrôles python fondés sur une dataframe panda ont été réduits et convertis en SQL lorsque possible pour de meilleurs performances

**📉 Régressions**

* Import des GeoJSON
* Tag des imports à corriger
* Alimentation des champs additionnels avec plusieurs colonnes
* Affichage du nombre total de données du fichier source dans la liste des imports
* Export / Import des modèles d'import, remplacé par la gestion des modèles d'import dans l'Admin de GeoNature
* Notification par email de la fin des opérations asynchrones (contrôles et import des données)

**⚠️ Notes de version**

* Suivez la procédure classique de mise à jour du module
* Exécutez la commande suivante afin d’indiquer à Alembic que votre base de données est dans l'état de la version 1.2.0 et appliquer automatiquement les évolutions pour la passer dans l'état de la version 2.0.0 :

::

   cd
   source geonature/backend/venv/bin/activate
   geonature db stamp 4b137deaf201
   geonature db autoupgrade

* Redémarrez le worker Celery :

::

  sudo systemctl restart geonature-worker
   

1.2.0 (2022-03-21)
------------------

Nécessite la version 2.9 de GeoNature. Non compatible avec les versions 2.10 et supérieures de GeoNature.

**🚀 Nouveautés**

* Ajout d'un rapport d'import - consultable en ligne et exportable en PDF - en cohérence avec le module métadonnées (#158)
* Affichage dynamique du nombre de données importées par rang taxonomique sous forme de graphique dans le rapport d'import et son export pdf (rang par défaut configurable avec le nouveau paramètre ``DEFAULT_RANK_VALUE``) (#221)
* Possibilité de taguer un import nécessitant des corrections et d'y attribuer un commentaire le cas échéant (#230)
* Possibilité de filtrer les imports nécessitant des corrections depuis la liste des imports (#189)
* Possibilité d'alimenter le champs "additionnal_data" de la synthèse avec un ou plusieurs champs du fichier source (#165)
* Possibilité de restreindre les imports à une aire géographique du ref_geo (configurable avec le nouveau paramètre ``ID_AREA_RESTRICTION``) : les données hors du territoire configuré sont mises en erreur (#217)
* Possibilité de restreindre les imports à une liste de taxons (configurable avec le nouveau paramètre ``ID_LIST_TAXA_RESTRICTION``) : les données ne portant pas sur ces taxons sont mises en erreur (#217)
* Affichage du nombre de données importées / nombre total dans la liste des imports (#183)
* Possibilité d'exporter ou d'importer des mappings en JSON pour les échanger entre instances de GeoNature (#146)

**🐛 Corrections**

* Suppression du champs "gn_is_valid" dans les tables d'import : les lignes invalides sont déduites à partir des erreurs détectées pour chaque donnée (gn_invalid_reason) (#223)
* L'étape 3 (mapping de nomenclatures) est désormais passée automatiquement si aucun champs de nomenclature n'a été rempli à l'étape précédente (mapping des champs) (#157)
* Suppression du rapport d'erreur au profit du rapport d'import plus complet, visuel et exportable (#158)
* Correction de l'autocomplétion de la recherche (#214)
* Amélioration du modèle de données : ajout d'une clé étrangère entre imports (gn_import.t_imports) et sources de la syntèse (gn_synthese.t_sources) (#201)
* Correction de la version setuptools lors de l'installation (#244)
* Compatilité Debian10 et Debian11

**Notes de version**

* Exécuter les fichiers de mise à jour du schéma de la BDD du module (``data/migration/1.1.8to1.2.0.sql``)

1.1.8 (2022-02-23)
------------------

**🐛 Corrections**

* Correction des performances d'import liées à la sérialisation récursive (#262 / #278)

1.1.7 (2022-01-13)
------------------

Nécessite la version 2.9.0 (ou plus) de GeoNature

**💻 Evolutions**

* Compatibilité avec GeoNature version 2.9.0 et plus.
* Révision du formulaire de mapping des nomenclatures pour l'adapter au passage à la libraire ``ng-select2`` dans la version 2.9.0 de GeoNature
* Limitation des jeux de données à ceux associés au module et en se basant sur l'action C du CRUVED du module (#267)

**⚠️ Notes de version**

* La liste des JDD associable à un import se base désormais sur le C du CRUVED de l'utilisateur au niveau du module (ou du C du CRUVED de GeoNature si l'utilisateur n'a pas de CRUVED sur le module), au lieu du R de GeoNature jusqu'à présent. Vous devrez donc potentiellement adapter vos permissions à ce changement de comportement (#267)

1.1.6 (2022-01-03)
------------------

Compatible avec Debian 10, nécessite des mises à jour des dépendances pour fonctionner sur Debian 11

**🐛 Corrections**

* Correction des performances de la liste des imports (#254)
* Optimisation du json chargé pour afficher la liste des imports
* Correction des rapports d'erreurs
* Versions des dépendances ``setuptools`` et ``pyproj`` fixées (#244)

1.1.5 (2021-10-07)
------------------

Nécessite la version 2.8.0 (ou plus) de GeoNature

**🚀 Nouveautés**

* Compatibilité avec Marshmallow 3 / GeoNature 2.8.0

1.1.4 (2021-06-30)
------------------

**🐛 Corrections**

* Correction du parsing des dates dans le cas où il y a une date mais pas d'heure, alors qu’on a mappé un champs d'heure

1.1.3 (2021-06-29)
------------------

**🐛 Corrections**

* Correction du contrôle des UUID quand ils sont fournis dans le fichier source

1.1.2 (2021-03-10)
------------------

**🐛 Corrections**

* Mise à jour du champs ``reference_biblio`` dans la table ``dict_fields`` (accepte 5.000 caractères depuis GeoNature 2.6.0)
* Correction du bug de calcul des UUID et des altitudes, et de l'activation de leur checkbox (#210, #211)

**Notes de version**

* Exécuter les fichiers de mise à jour du schéma de la BDD du module (``data/migration/1.1.1to1.1.2.sql``)
* Si vous avez fait des imports depuis la version 1.1.1, vous pouvez jouer le script ``data/migration/generate_uuid.sql``. Attention, celui-ci regénère des nouveaux UUID dans la synthese pour toutes les données provenant du module Import où le champs ``unique_id_sinp`` est ``NULL``

1.1.1 (2020-02-04)
------------------

Attention : le module d'import 1.1.1 nécessite la version 2.6.0 de GeoNature. Faire la MAJ de GeoNature dans un premier temps.

**🚀 Nouveautés**

* Ajout de la notion de mappings "publics" (champs ``is_public boolean DEFAULT FALSE`` de la table ``t_mappings``). Tous les utilisateurs verront ces mappings qui ne seront modifiables que par les utilisateurs ayant des droits U=3 ainsi que leurs créateurs (#98)
* Création d'une documentation listant tous les contrôles - https://github.com/PnX-SI/gn_module_import/blob/develop/docs/controls.md (#17)
* Performances de l'insertion dans la synthèse : suppression des post-traitements de calcul des couleurs des taxons par unités géographiques, convertis en vue dans GeoNature 2.6.0, et optimisation des calculs des intersections des observations avec les zonages
* Ajout de contrôles conditionnels sur ``TypeInfoGeo`` et de paramètres permettant de désactiver les contrôles conditionnels (#176 et #171)
* Clarification des paramètres du fichier d'exemple de configuration (``config/conf_gn_module.toml.example``)
* Ajout de paramètres
* Rapport d'erreur : Affichage des vocabulaires de nomenclature en erreur
* Etape 4 : Séparation des alertes et des erreurs

**🐛 Corrections**

* Liste des imports : Retour du bouton permettant de télécharger les éventuelles lignes en erreur d'un import terminé (#169)
* Correction des vérifications du CRUVED sur la liste des imports (#120)
* Correction de la récupération du CRUVED sur les mappings
* Si des lignes sont vides pour une colonne de nomenclature mappée, alors on insère la valeur par défaut définie dans la BDD
* Masquage du bouton d'import si l'action C du CRUVED de l'utilisateur est égale à zéro (#95)
* Correction et clarification des messages d'erreurs affichés à l'utilisateur (#83)
* Suppression de l'erreur 404 à l'étape 2 quand l'utilisateur n'a aucun mapping (#136)
* Correction de la modification du SRID (#180)
* Correction des altitudes quand on utilise le même champs source pour les altitudes min et max (#194)
* Correction de l'affichage du message "Import en erreur" si l'import est corrigé (#195)
* Correction de la vérification des dates
* Correction des imports des heures
* Correction d'une erreur causée quand les noms des champs de nomenclatures sont trop longs (#198)

**Notes de version**

* Si vous mettez à jour le module depuis sa version 1.1.0, exécuter les fichiers de mise à jour du schéma de la BDD du module (``data/migration/1.1.0to1.1.1.sql``) et suivez la procédure habituelle : https://github.com/PnX-SI/gn_module_import#mise-%C3%A0-jour-du-module
* NB : la procédure de MAJ a été revue : bien exécuter la commande ``pip install -r /home/`whoami`/gn_module_import/backend/requirements.txt`` (depuis le virtualenv de GeoNature) comme indiqué

1.1.0 (2020-11-05)
------------------

Nécessite GeoNature 2.5.3 minimum.

**🚀 Nouveautés**

* Ajout des champs du standard Occtax V2 (#163)
* Ajout et mise à jour des champs de la synthèse (modifiés depuis GeoNature 2.5.0)
* Mise à jour et complément des modèles d'import fournis par défaut ("Format DEE 10 caractères" et "Synthèse GeoNature")
* Possibilité de supprimer un import (et les données associées) (#124)
* Ajout de la possibilité de ne pas afficher l'étape "Mapping des contenus" en définissant un mapping par défaut (avec les paramètres ``ALLOW_VALUE_MAPPING`` et ``DEFAULT_VALUE_MAPPING_ID``) (#100)
* Import possible des données sans géométrie en utilisant les colonnes ``codecommune``, ``codemaille`` ou ``codedepartement`` et en récupérant ``id_area`` et leur géométrie correspondantes dans la couche des zonages du ``ref_geo`` (#107)
* Implémentation du CRUVED pour identifier si l'utilisateur peut modifier ou créer un mapping. Les mappings sont un objet dont le CRUVED est paramétrable (module Admin -> Permissions) (#136)
* Création de mappings temporaires supprimés automatiquement à la fin d'un import, pour les utilisateurs n'ayant pas les droits de modifier ou créer des mappings (#136)
* Implémentation du CRUVED sur la liste des imports (#120)
* Renommage des intitulés (#122). "Mapping" devient notamment "Modèle d'import" et "Correspondance"
* Parallélisation des traitements et des contrôles à partir d'un seuil paramétrable de nombre de lignes dans le fichier importé (``MAX_LINE_LIMIT``) (#123)
* Envoi d'un email à l'auteur d'un import quand les contrôles réalisés en parallèle sont terminés (#123)
* Simplification des étapes d'import pour les non-administrateurs (#113)
* Révision et complément des contrôles des données et amélioration des rapports d'erreurs (#114)
* Regroupement du contrôle des données après l'étape de mapping des valeurs, avant l'étape de prévisualisation des données à intégrer
* Ajout d'un tableau d'erreur à la première étape d'upload du fichier
* Ajout d'un rapport d'erreur consultable à la dernière étape avant intégration des données et depuis la liste des imports
* Ajout de contrôles, sur les champs conditionnels et les géométries notamment (validité et bounding box) (#130)
* Ajout du paramètre ``INSTANCE_BOUNDING_BOX`` pour définir les coordonnées de la bounding box de contrôle de la géométrie des données (en 4326 * WGS84) (#130)
* Ajout des paramètres ``ENABLE_BOUNDING_BOX_CHECK`` et ``ENABLE_SYNTHESE_UUID_CHECK`` pour activer ou non les contrôles de bounding box et d'UUID qui peuvent être chronophages
* Enregistrement et affichage des lignes du fichier source en erreur
* Ajout d'une vue ``gn_imports.v_imports_errors`` permettant de lister les erreurs d'un import
* Ajout du paramètre ``FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE`` pour remplir ou non les nomenclatures en erreur par la valeur par défaut définie dans la BDD
* Prévisualisation des données avant intégration : Ajout d'une carte avec la bounding box des données (#58)
* Liste des imports : Ajout d'une recherche libre et du tri des colonnes (#75)
* Liste des imports : Ajout des colonnes "Auteur", "Nombre de données" et "Nombre de taxons" (paramétrable comme les autres colonnes) (#92)
* Liste des imports : Ajout d'un lien vers la fiche du JDD correspondant
* Séparateur des fichiers CSV importés détectés automatiquement (#119)
* Ajout des champs ``uuid_autogenerated`` et ``altitude_autogenerated`` dans la table ``gn_imports.t_imports``
* Documentation de l'utilisation et du fonctionnement du module
* Documentation de la mise à jour du module (#149)
* Ajout de la correspondance au standard SINP sur l'ensemble des champs du mapping dans une tooltip

**🐛 Corrections**

* Refactoring et révision globale des performances du code
* Désactivation des triggers de la Synthèse avant insertion des données pour améliorer les performances, éxecution globale des actions des triggers puis réactivation des triggers après insertion des données
* Prévisualisation des données avant intégration : Affichage des labels des nomenclatures et non plus de leurs codes
* Correction du modèle d'import "Synthèse GeoNature" fournis par défaut (#118)
* Suppression du message d'erreur quand un champs défini dans un mapping n'est pas présent dans le fichier importé (#108)
* Correction et amélioration des contrôles de dates, pouvant être fournis dans différents formats (#128)
* Suppression temporaire de la vérification des doublons dans le fichier source, trop lourde en performance et non fonctionnelle
* Clarification de l'intitulé et masquage par défaut du champs "id_digitiser" (#159)
* Correction de la génération des UUID SINP (#156)
* Correction de la génération des altitudes (#155)
* Correction de la vérification de la bounding box (#151)
* Ajout d'une vérification sur la longueur des fichiers fournis (50 caractères)
* Transformation des nomenclatures dans des colonnes séparées (#148)
* Vérification que l'utilisateur a bien un email renseigné

**Notes de version**

* Si vous mettez à jour depuis la version 1.0.0, exécuter les fichiers de mise à jour du schéma de la BDD du module (``data/migration/1.0.0to1.1.0.rc.2.sql`` puis ``data/migration/1.1.0.rc.2to1.1.0.sql``)
* Si vous mettez à jour depuis la version 1.1.0.rc.2, exécuter le fichier de mise à jour du schéma de la BDD du module (``data/migration/1.1.0.rc.2to1.1.0.sql``)
* Vérifier les éventuelles nouveaux paramètres que vous souhaiteriez surcoucher dans le fichier ``config/conf_gn_module.toml`` à partir du fichier d'exemple ``config/conf_gn_module.toml.example``
* Si vous activez la parallélisation des contrôles (``MAX_LINE_LIMIT``) (#123), assurez-vous d'avoir défini les paramètres d'envoi d'email dans la configuration globale de GeoNature (``geonature/config/geonature_config.toml``)

1.0.0 (2020-02-26)
------------------

A vos marques, prêts, importez !

**🚀 Nouveautés**

* Précision au survol sur l'icone de téléchargement des données invalides (étape 1) (#62)
* Ajout d'un mapping par défaut pour les données issues de la Synthèse GeoNature et les nomenclatures/codes du SINP correspondant aux champs de la synthèse
* Sérialisation des identifiants dans la BDD du module (#82)
* Scission des fichiers SQL d'installation de la BDD, des données obligatoires, et des données de mapping par défaut 

**🐛 Corrections**

* Compatibilité avec GeoNature 2.3.1
* Corrections du mapping de contenus et composant multiselect (#85 et #71)
* Contrôle des doublons sur les UUID fournis dans les données sources
* Prise en charge des UUID sources lorsqu'ils sont importés en majuscule (#61)
* Correction de la récupération des jeux de données en fonction de l'utilisateur et limitation aux JDD actifs (#79)
* Correction du calcul du nombre de taxons importés (basé sur le ``cd_ref`` et non plus sur le ``cd_nom``) (#60)
* Masquage des champs obligatoires rendu impossible dans la configuration de l'interface (#53)
* Ajout de la constante ``I`` (Insert) dans le champ ``last_action`` de la synthèse lors de l'import (#52)
* Correction du chemin du répertoire upload (#46)
* Nom du module repassé en variable lors de l'installation (#47)
* Champs ``WKT (Point)`` renommé ``WKT`` (prend en charge les lignes et polygones)
* Versions de ``toolz`` et ``cloudpickle`` fixées dans ``requirements.txt`` (#70 et #80)
* Suppression du doublon de la colonne "date d'import" dans l'interface de l'étape 1
* Ajout de clés étrangères manquantes (#81)
* Ajout du champs ``unique_id_sinp_grp`` dans la configuration par défaut du module (#67)
* Correction du contrôle de cohérence des coordonnées géographiques pour les WKT (#64)

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
