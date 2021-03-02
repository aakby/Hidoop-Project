# Hidoop-Project

Ce project s'inspire de la framework libre et opensource Hadoop dans le but de faciliter
la création et la gestion des applications dstribuées.

# Partie HDFS

Le système de fichiers HDFS est utilisé pour gérer les accès massives et concurrents à des fichiers de large
volume. En effet, un fichier est fragmenté en blocs de tailles identiques qui sont dupliqués pour des fins
de pannes des serveurs HDFS. Pour cette version, le facteur de duplication repFactor est considéré comme
valant 1 (pas de duplication).

# Partie Hidoop

Le service Hidoop permet de gérer l’exécution repartie, parallèle et concurrente des traitements des Map sur
différentes machines, ensuite de récupérer leurs résultats et de leur appliquer un Reduce.
Il s’agit en effet de programmer deux packages : Ordo et Map qui contienne principalement des classes
Worker, CallBack et JobLauncher.
