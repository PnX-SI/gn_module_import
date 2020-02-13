Installation du module d'imports
==================================

Télécharger le module (version test sur la branche develop)

::

   cd
   wget https://github.com/PnX-SI/gn_module_import/archive/X.Y.Z.zip
   unzip X.Y.Z.zip
   mv gn_module_import-X.Y.Z gn_module_import


Le temps d'une correction à venir, il est nécessaire d'installer manuellement les pré-requis du module avant de procéder à son installation en le nommant "import" (pensez à corriger les chemins si votre utilisateur ne s'appelle pas geonatureadmin)

::

   cd
   source geonature/backend/venv/bin/activate
   geonature install_gn_module /home/`whoami`/gn_module_import import
   deactivate
   
   
Le module est installé et prêt à être testé.
 

Configuration du module
=======================
TODO

Il est également possible de paramétrer l'ordonnancement des champs (ordre, regroupements dans des blocs) dans l'interface du mapping de champs. A l'instar des attributs gérés dans taxhub, il est possible de définir des "blocs" dans la table gn_imports.dict_themes, et d'y attribuer des champs (dict_fields) en y définissant leur ordre de rangement. Les champs sont également masquables via le champs booléen "display". Un champs masqué sera traité comme un champs non rempli, et se verra associer des valeurs par défaut ou une information vide. Masquer les champs obligatoires rendra donc impossible l'import de données. 

Après avoir regroupé les champs dans leurs "blocs" et leur avoir associé un rang, vous devrez relancer le build de l'interface. 

::

   cd
   cd geonature/frontend
   npm run build


Fonctionnement du module d'imports
==================================

Le module permet de traiter un fichier csv ou json sous tout format de données, d'établir les correspondances nécessaires, et d'intégrer les données dans la synthèse de GeoNature. Il semble préférable de prévoir un serveur disposant a minima de 4Go de RAM. 


::
   Financement : DREAL et Conseil Régional Auvergne-hône-Alpes
