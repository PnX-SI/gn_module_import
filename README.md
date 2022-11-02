Module d’import
===============

| branche | tests | coverage |
|---|---|---|
| refactor | [![pytest](https://github.com/PnX-SI/gn_module_import/actions/workflows/pytest.yml/badge.svg?branch=refactor)](https://github.com/PnX-SI/gn_module_import/actions/workflows/pytest.yml) | [![codecov](https://codecov.io/gh/PnX-SI/gn_module_import/branch/refactor/graph/badge.svg?token=BU2G1MN5XH)](https://codecov.io/gh/PnX-SI/gn_module_import) |

Ce module permet d’importer des données depuis un fichier CSV dans [GeoNature](https://github.com/PnX-SI/GeoNature).

Installation du module d’imports
================================

Télécharger puis renommer la version souhaitée du module :

```
    cd
    wget https://github.com/PnX-SI/gn_module_import/archive/X.Y.Z.zip
    unzip X.Y.Z.zip
    rm X.Y.Z.zip
    mv gn_module_import-X.Y.Z gn_module_import
```

Le module doit ensuite être installé comme suit :

```
    cd
    source geonature/backend/venv/bin/activate
    geonature install-packaged-gn-module gn_module_import IMPORT
    deactivate
    sudo systemctl restart geonature
    cd geonature/frontend
    nvm use
    npm run build
```

Le module est installé et prêt à importer !

Configuration du module
=======================

La configuration du module se fait pour partie via le fichier
`conf_gn_module.toml`. Voir le fichier `conf_gn_module.toml.example`
pour voir la liste des paramètres disponibles (champs affichés en
interface à l'étape 1, préfixe des champs ajoutés par le module,
répertoire d'upload des fichiers, SRID, encodage, séparateurs, etc).

Configuration avancée
---------------------

Une autre partie se fait directement dans la base de données, dans les
tables `dict_fields` et `dict_themes`, permettant de masquer, ajouter,
ou rendre obligatoire certains champs à renseigner pour l'import. Un
champs masqué sera traité comme un champs non rempli, et se verra
associer des valeurs par défaut ou une information vide. Il est
également possible de paramétrer l'ordonnancement des champs (ordre,
regroupements dans des blocs) dans l'interface du mapping de champs. A
l'instar des attributs gérés dans TaxHub, il est possible de définir
des "blocs" dans la table `gn_imports.dict_themes`, et d'y attribuer
des champs (`dict_fields`) en y définissant leur ordre d'affichage.

Droits du module
================

La gestions des droits dans le module d'import se fait via le réglage
du CRUVED à deux niveaux : au niveau du module d'import lui-même et au
niveau de l'objet "mapping".

-   Le CRUVED du module d'import permet uniquement de gérer
    l'affichage des imports. Une personne ayant un R = 3 verra tous les
    imports de la plateforme, un R = 2 seulement ceux de son organisme
    et un R = 1 seulement les siens.
-   Les jeux de données selectionnables par un utilisateur lors de la
    création d'un import sont eux controlés par les permissions
    globales de GeoNature (et non du module d'import).
-   Les mappings constituent un "objet" du module d'import disposant
    de droits paramétrables pour les différents rôles. Par défaut, les
    droits accordés sur les mappings sont identiques aux droits que les
    utilisateurs ont sur le module Import en lui-même. Le réglage des
    droits se fait dans le module "Admin" de GeoNature ("Admin" -\>
    "Permissions").
-   Certains mappings sont définit comme "public" et sont vu par tout
    le monde. Seul les administrateurs (U=3) et les propriétaires de ces
    mappings peuvent les modifier.
-   Avec un C = 1, R = 2, U = 1 un utilisateur pourra par exemple créer
    des nouveaux mappings (des modèles d'imports), modifier ses propres
    mappings et voir les mappings de son organismes ainsi que les
    mappings "publics".
-   Si vous modifiez un mapping sur lequel vous n'avez pas les droits,
    il vous sera proposé de créer un nouveau mapping vous appartenant
    avec les modifications que vous avez faites, mais sans modifier le
    mapping initial. Lorsqu'on a les droits de modification sur un mapping,
    il est également possible de ne pas enregistrer les modifications
    faites à celui-ci pour ne pas écraser le mapping inital (le mapping
    sera sauvegardé uniquement avec l’import).

Contrôles et transformations
============================

[Liste des contrôles](docs/controls.rst)

Mise à jour du module
=====================

-   Téléchargez la nouvelle version du module

```
    wget https://github.com/PnX-SI/gn_module_import/archive/X.Y.Z.zip
    unzip X.Y.Z.zip
    rm X.Y.Z.zip
```

-   Renommez l'ancien et le nouveau répertoire

```
    mv ~/gn_module_import ~/gn_module_import_old
    mv ~/gn_module_import-X.Y.Z ~/gn_module_import
```

-   Rapatriez le fichier de configuration

```
    cp ~/gn_module_import_old/config/conf_gn_module.toml  ~/gn_module_import/config/conf_gn_module.toml
```

-   Relancez la compilation en mettant à jour la configuration

```
    cd ~/geonature/backend
    source venv/bin/activate
    pip install -e ~/gn_module_import/
    geonature update-module-configuration IMPORT
    sudo systemctl restart geonature
    cd ~/geonature/frontend
    nvm use
    npm run build
```

Utilisation du module d'imports
================================

Note : le processus a un petit peu évoluer en v2 avec notamment une
étape supplémentaire.

Le module permet de traiter un fichier CSV ou GeoJSON sous toute
structure de données, d'établir les correspondances nécessaires entre
le format source et le format de la synthèse, et de traduire le
vocabulaire source vers les nomenclatures SINP. Il stocke et archive les
données sources et intègre les données transformées dans la synthèse de
GeoNature. Il semble préférable de prévoir un serveur disposant à minima
de 4 Go de RAM.

1.  Une fois connecté à GeoNature, accédez au module Imports. L'accueil
    du module affiche une liste des imports en cours ou terminés, selon
    les droits de l'utilisateur connecté. Vous pouvez alors finir un
    import en cours, ou bien commencer un nouvel import.

![image](https://geonature.fr/docs/img/import/gn_imports-01.jpg)

2.  Choisissez à quel JDD les données importées vont être associées. Si
    vous souhaitez les associer à un nouveau JDD, il faut l'avoir créé
    au préalable dans le module métadonnées.

![image](https://geonature.fr/docs/img/import/gn_imports-02.jpg)

3.  Chargez le fichier CSV ou GeoJSON à importer.

![image](https://geonature.fr/docs/img/import/gn_imports-03.jpg)

4.  Mapping des champs. Il s'agit de faire correspondre les champs du
    fichier importé aux champs de la Synthèse (basé sur le standard
    "Occurrences de taxons" du SINP). Vous pouvez utiliser un mapping
    déjà existant ou en créer un nouveau. Le module contient par défaut
    un mapping correspondant à un fichier exporté au format par défaut
    de la synthèse de GeoNature. Si vous créez un nouveau mapping, il
    sera ensuite réutilisable pour les imports suivants. Il est aussi
    possible de choisir si les UUID uniques doivent être générés et si
    les altitudes doivent être calculées automatiquement si elles ne
    sont pas renseignées dans le fichier importé.

![image](https://geonature.fr/docs/img/import/gn_imports-04.jpg)

6.  Une fois le mapping des champs réalisé, au moins sur les champs
    obligatoires, il faut alors valider le mapping pour lancer le
    contrôle des données. Vous pouvez ensuite consulter les éventuelles
    erreurs. Il est alors possible de corriger les données en erreurs
    directement dans la base de données, dans la table temporaire des
    données en cours d'import, puis de revalider le mapping, ou de
    passer à l'étape suivante. Les données en erreur ne seront pas
    importées et seront téléchargeables dans un fichier dédié à l'issue
    du processus.

![image](https://geonature.fr/docs/img/import/gn_imports-05.jpg)

7.  Mapping des contenus. Il s'agit de faire correspondre les valeurs
    des champs du fichier importé avec les valeurs disponibles dans les
    champs de la Synthèse de GeoNature (basés par défaut sur les
    nomenclatures du SINP). Par défaut les correspondances avec les
    nomenclatures du SINP sous forme de code ou de libellés sont
    fournies.

![image](https://geonature.fr/docs/img/import/gn_imports-06.jpg)

8.  La dernière étape permet d'avoir un aperçu des données à importer
    et leur nombre, avant de valider l'import final dans la Synthèse de
    GeoNature.

![image](https://geonature.fr/docs/img/import/gn_imports-07.jpg)

Pour chaque fichier importé, les données brutes sont importées
intialement et stockées tel quel dans une table portant le nom du
fichier, dans le schéma `gn_import_archives`. Elles sont aussi stockées
dans une table intermédiaire, enrichie au fur et à mesure des étapes de
l'import.

Liste des contrôles réalisés sur le fichier importé et ses données :
<https://github.com/PnX-SI/gn_module_import/issues/17>

Schéma (initial et théorique) des étapes de fonctionnement du module :

![image](https://geonature.fr/docs/img/import/gn_imports_etapes.png)

Modèle de données du schéma `gn_imports` du module :

![image](https://geonature.fr/docs/img/import/gn_imports_MCD-2020-03.png)


Financement de la version 1.0.0 : DREAL et Conseil Régional
Auvergne-Rhône-Alpes.
