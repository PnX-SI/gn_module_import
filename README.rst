Installation du module d'imports
==================================

Télécharger le module (version test sur la branche develop)

::

   cd
   wget https://github.com/PnX-SI/gn_module_import/archive/develop.zip
   unzip develop.zip


Le temps d'une correction à venir, il est nécessaire d'installer manuellement les pré-requis du module avant de procéder à son installation en le nommant "import"

::

   cd
   source geonature/backend/venv/bin/activate
   pip install goodtables==2.1.4 pandas==0.24.2 dask==0.19.1 dask[dataframe]==2.0.0 geopandas==0.5.1 psutil==5.4.7
   geonature install_gn_module /home/geonatureadmin/gn_module_import-develop import
   deactivate
   
   
Le module est installé et prêt à être testé.
 


Fonctionnement du module d'imports
==================================

Le module permet de traiter un fichier csv ou json sous tout format de données, d'établir les correspondances nécessaires, et d'intégrer les données dans la synthèse de GeoNature. Il semble préférable de prévoir un serveur disposant a minima de 4Go de RAM. 


