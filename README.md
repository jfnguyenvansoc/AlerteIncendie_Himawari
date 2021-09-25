# AlerteIncendie_Himawari
Traitement Himawari pour le systeme d'alerte incendie NC
Chaine de traitement des points d’anomalies thermiques HIMAWARI de l’agence JAXA

1.	Localisation des ressources
1.1	Serveur
Le traitement est localisé et s’effectue sur le serveur geoportail.oeil.nc.
1.2	Répertoire de traitement
Le principal répertoire est : « D:\ALERTE_INCENDIE_ENVIRONNEMENTALE\NC\Production\HIMAWARI_PROCESS »
1.3	Script
Le script en Python se nomme « TraitementHimawariSDE.py ».
1.4	Fichier de configuration du script
Les informations de configuration (serveur FTP, connexion aux bases, …) se trouvent dans le fichier « Config.json » et sont utilisées par le script en python.
1.5	Bases de données utilisées
Les bases de données utilisées sont : 
1.5.2	Géodatabase SDE
1)	la geodatabase SDE « ALERTE_INCENDIE_v2 » sur l’instance « EC2AMAZ-BKQD7P5\VULCAINNCPROD », la connexion utilisée est celle SDE dans le répertoire « D:\ALERTE_INCENDIE_ENVIRONNEMENTALE\NC\Production\ALERTE_INCENDIE.sde » est celle qui contient les couches de données spatiales mises à jour.
1.5.3	Géodatabase Fichier
2)	la geodatabase fichier « HimawariBD.gdb » permet de stocker des tables de suivi pour savoir quels sont les répertoires et fichiers CSV sont déjà traités.
1.6	Serveur FTP distant
Le serveur FTP distant est celui mis à disposition par l’agence spatiale japonaise « JAXA » sur ftp.ptree.jaxa.jp les détails de connexion sont dans le fichier de configuration.
2.	Description des étapes de traitement
Le script est lancé via un fichier HimawariNC.bat dans le répertoire de traitement qui permet d’intégrer l’exécution du script et la commande FME permettant ensuite de mettre à jour les incendies pour les statistiques VulcainPro et d’envoyer ensuite une alerte si nécessaire.

Le fichier HimawariNC.bat est lancé via une tâche planifiée dans le planificateur de tâches du serveur nommée « HimawariProcess » localisé sous « Alertes Incendie > Production », exécutée toutes les 10 minutes.

L’organisation des fichiers CSV sur le serveur FTP de l’agence spatiale japonaise est organisée en 3 niveaux : 1) répertoires par mois, 2) répertoires par jour, 3) répertoire par heure, et sous chaque répertoire par heure, les fichiers CSV contiennent les données générées d’anomalies thermiques toutes les 10 minutes par heure.
Pour conserver l’historique déjà récupérés, des tables permettent de tracer les répertoires déjà traités par mois (Table RepMois), par jour (Table RepJour), et par heure (Table RepHeure), mais également les noms de fichiers CSV déjà traités (Table CSVTraites) situées dans la géodatabase fichier.

Lorsqu’un fichier CSV doit être traité, celui-ci est téléchargé temporairement dans le répertoire « Download » localisé sous le répertoire de traitement, et il est ensuite supprimé lorsqu’il a été traité puis son nom est ensuite renseigné comme fichier traité dans la table « CSVTraites ». 
Le script permet donc de vérifier avec les coordonnées présentes de chaque point s’ils sont à l’intérieur d’une étendue spatiale de la NC, renseignée dans le fichier de configuration. 
Si un point correspond est à l’intérieur de l’étendue spatiale NC, on vérifie ensuite s’il est à l’intérieur des zones de la couche « contour_NC_sans_doniambo_KNS_HIMAWARI_2KM_Final » (périmètre NC avec buffer de 2km mais en retirant les zones de Doniambo SLN et KNS) présent dans la géodatabase fichier.
Si ce point est bien à l’intérieur du périmètre NC, le script vérifie qu’avec les mêmes coordonnées du point, il n’y a pas déjà un point présent dans un délai temporel défini dans le fichier de configuration (ex : mêmes coordonnées d’un point avec moins de 30 minutes d’intervalle, le point n’est pas pris en compte).
Ensuite si le filtre temporel est satisfaisant, à l’aide du point il est reprojeté en RGNC 1991-93 puis pour une surface représentant théoriquement le pixel de 2km est créé à partir de ce point, puis ceux-ci sont intégrés dans la couche de point « fireDetectionPointsHIMAWARI » et la couche de polygones « incendies_HIMAWARI ».

Ensuite la commande suivante est lancée pour lancer une commande FME,
-	si c’est mode temps réel : "C:/Program Files/FME2019/fme.exe\" \"D:/ALERTE_INCENDIE_ENVIRONNEMENTALE/NC/Production/Firms/02_FME/00_General.fmw\" --SOURCE Himawari --MODE TempsReel,
-	ou si c’est en mode historique et qu’aucune alerte incendie doit être envoyée aux abonnés : "C:/Program Files/FME2019/fme.exe\" \"D:/ALERTE_INCENDIE_ENVIRONNEMENTALE/NC/Production/Firms/02_FME/00_General.fmw\" --SOURCE Himawari --MODE Historique

En parallèle une alerte est envoyée par e-mail aux administrateurs pour indiquer qu’il y a un ou des points qui seront intégrés en base sur le fichier CSV concerné, les adresses e-mails des destinataires sont à indiquer dans le fichier de configuration dans ToADDR et To ADDR2.
Pour l’envoi des emails le compte « user@oeil.nc » est utilisé.

