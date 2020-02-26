Installation du module d'imports
==================================

Télécharger puis renommer la version actuelle du module :

::

   cd
   wget https://github.com/PnX-SI/gn_module_import/archive/X.Y.Z.zip
   unzip X.Y.Z.zip
   mv gn_module_import-X.Y.Z gn_module_import


Le module doit ainsi être installé comme suit :

::

   cd
   source geonature/backend/venv/bin/activate
   geonature install_gn_module /home/`whoami`/gn_module_import import
   deactivate
   
   
Le module est installé et prêt à importer!
 

Configuration du module
=======================

La configuration du module se fait pour partie via le fichier ``conf_schema_toml.py`` (champs affichés en interface à l'étape 1, préfixe des champs ajoutés par le module, répertoire d'upload des fichiers, SRID, encodage, séparateurs etc). 

Une autre partie se fait directement via la base de données, dans les tables ``dict_fields`` et ``dict_themes``, permettant de masquer, ajouter, ou rendre obligatoire certains champs à renseigner pour l'import. Un champs masqué sera traité comme un champs non rempli, et se verra associer des valeurs par défaut ou une information vide. Il est également possible de paramétrer l'ordonnancement des champs (ordre, regroupements dans des blocs) dans l'interface du mapping de champs. A l'instar des attributs gérés dans taxhub, il est possible de définir des "blocs" dans la table gn_imports.dict_themes, et d'y attribuer des champs (dict_fields) en y définissant leur ordre de rangement.  

Après avoir regroupé les champs dans leurs "blocs" et leur avoir associé un rang, vous devrez relancer le build de l'interface. 

::

   cd
   cd geonature/frontend
   npm run build


Fonctionnement du module d'imports
==================================

Le module permet de traiter un fichier csv ou json sous tout format de données, d'établir les correspondances nécessaires entre le format source et le format de la synthèse, et de traduire le vocabulaire source vers les nomenclatures SINP. Il intègre les données transformées dans la synthèse de GeoNature. Il semble préférable de prévoir un serveur disposant a minima de 4Go de RAM. 


::
   Financement : DREAL et Conseil Régional Auvergne-hône-Alpes.
