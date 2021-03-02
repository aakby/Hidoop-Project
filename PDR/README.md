# ordo
## CallBack
Il s'agit de l'interface qui représente le CallBack et qui hérite de la classe java.rmi.Remote

## CallBackImpl
Il s'agit de l'implementation de l'interface Callback et qui lorsque tous les fragments auront été éxécutés, le fait savoir.
## Worker
Il s'agit de l'interface du Demon Worker qui sera lancé sur chaque machine et qui de la classe java.rmi.Remote
## WorkerImpl 
Il s'agit de l'implementation de l'interface Worker, et qui contient les différents traitements nécessaires à l'appel du Démon ainsi que l'implementation d'une sous classe mapRunner qui lancera l'éxecution du Map
## Job
Dans cette classe, on trouve la méthode startJob qui permet de lancer les Map en appelant runMap sur les Worker tout en leur donnant un reader, writer et callback