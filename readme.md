# **Procédure de construction de la base de données désirée**

Ce document texte comprend la procédure pour utiliser le script

&NewLine;  
&NewLine;  

## **I. Setup l'environnement de travail**

### **I.1. Installer Python 3.5 ou plus.**

&NewLine;  
&NewLine;  

Le fichier installable ou la procédure peut être trouvé sur https://www.python.org

> **Note**: Il est possible que python soit déjà installé sur votre ordinateur verifier le en 
tapant la commande suivante dans votre terminal

&NewLine;  

```shell
python --version
```
&NewLine;  

> **Note**: Pour les utilisateurs Windows n'oublier pas de cliquer sur:
"Ajouter python aux variables d'environnement"
lors de l'installation ou alors faite un recherche pour trouver comment ajouter python aux
variable d'environnement

&NewLine;  
&NewLine;  

### **I.2. Installer les librairies necessaires**

&NewLine;  
&NewLine;  

Vérifier que pip est installé taper:

```shell
pip --version
```

ou

```shell
python3 -m pip --version
```

ou

```shell
python -m pip --version
```

ou 

```shell
py -m pip --version
```
&NewLine;  
&NewLine;  

> **Note:** Si aucune de ses commande ne fonctionne et vous retourne une erreur alors essayez de trouver sur internet commment installer pip ou copier et coller l'erreur sur google.com et chercher la reponse dans les proposition que vous obtiendez.

Si tous fonctionne correctement alors ouvrez votre terminal et saississez (ou copier-coller) la commande suivante:

```shell
pip install tqdm
```

ou 

```shell
python3 -m pip install tqdm
```

ou

```shell
pyhton -m pip install tqdm
```

&NewLine;  
&NewLine;  

### **I.3. Installation de jupyter (facultatif)**

&NewLine;  
&NewLine;  

Taper les commades suivante:

```shell
pip install jupyter 
```

et

```shell
pip install ipython
```

&NewLine;  
&NewLine;  

## **II. Télécharger les bases de données wikipedia nécésaire**

&NewLine;  
&NewLine;  

La base de données complète du wikipédia russe est disponible à l'adresse suivante: https://dumps.wikimedia.org/ruwiki/latest/


Les bases de données nécéssaire à téléchargér sont les suivantes: 

* ruwiki-latest-page.sql.gz
* ruwiki-latest-langlinks.sql.gz

Elles devraient normalement parmis les 100 premiers résultats. Il faut juste cliquez sur le lien correspondant pour démarrer le téléchargement.

> **Note:** Enregistrez les dans le dossier du projet (le dossier où se trouvent les scripts python à executer).


&NewLine;  
&NewLine;  

## **III. Execusion**

### **III.1. Dans terminal**

&NewLine;  
&NewLine;  


Ici plusieurs possibilités sont offertes chacune avec ses avantages et inconveniants:

1. lancez directement le script : extract_and_compute_data.py
2. effectuer les étapes sont les suivantes:
    * executer le script : extract_data.py
    * puis : compute_and_save_decoded.py

> **Note:**
* Dans le premiers cas si un problème survient il faudra supprimer le dossier results et tout recommancer. Mais une fois démarrer le script produits directement la base de données attendue.

* Dans le second cas il faudra recommencer uniquement  l'étape qui pose problème. Mais une présence humaine est necessaire pour lancer chaque script.

&NewLine;  
&NewLine;  

> **Note:**
* Si vous choississez la première étape et que tous se passe bien alors les resltats seront dans le dossier result  

* Si vous choississez la deuxieme étapes, alors les résultats seront dans le dossier dedoced et dans ce cas vous
pourrez supprimer le dossier data.

&NewLine;  
&NewLine;  

> **Note:** Pour executer le script
* Si vous êtes sur Linux ou MacOs (ubunut ou autres) faite un clic-droit dans le dossier du projet et choisir ouvrir dans le terminal. Puis ecrivez python3 <<nom du scrpt à executer>>.py ex: pyhton3 extract_and_compute_data.py

* Si vous ete sur Windows:

    * Ouvrez le dossier du projet et copier le texte qui se trouve dans la barre de naviguation. Ou alors cliquez sur n'importe quel fichier et allez dans ses propriétés et à la ligne emplacement copier le texte qui suit (C:\...)

    * Ouvrez l'invite de commande puis ecrivez "cd" (sans les guillements). Faites ensuite un click droit et enfin appuyez sur entrée.

    * Ecrivez python <<nom du scrpt à executer>>.py 
        ex: pyhton extract_and_compute_data.py

Attendez la fin de l'execution du script marquée par un "End of Processing" et passez ou non au suivant.

&NewLine;  
&NewLine;  

### **III.2. Dans Jupyter Note book**

&NewLine;  
&NewLine;  

Il suffit d'executer les cellules une à une jusqu'a la dernier celle. 

>**Note:** Pas besoin de réésexuter les cellules qui n'ont pas genérer d'erreurs. Mais si pour une raison où une autre le notebook se 
ferme il faut tout réexécuter. Si l'epate 1 c'est bien effectuer, n'exécuter que l'étape 2.

>**Note:** Une étape c'est bien éfféctué lorsqu'il est marqué juste en dessous (et hors de la cellule) "End of this step".


&NewLine;  
&NewLine;  

**Merci!**

&NewLine;  
&NewLine;  

<div style="margin: 1em;">
    <span style="background-color: rgb(120, 120, 255); padding:5px; border-radius: 5px;">Author :</span>
    <span style="background-color: rgba(0, 0, 0, .5); padding:5px; border-radius: 5px 10px;">Ovide Kuichua</span>  
</div>
<div style="margin: 1em;">
        <span style="background-color: rgb(120, 120, 255); padding:5px 10px; border-radius: 5px;"> Email :</span> <span style="background-color: rgba(0, 0, 0, .5); padding:5px; border-radius: 5px;">bertrandovide@hotmail.com</span>
</div>
<div style="margin: 1em;">
        <span style="background-color: rgb(120, 120, 255); padding:5px; border-radius: 5px;"> LinkedIn :</span>
        <span style="background-color: rgba(0, 0, 0, .5); padding:5px 10px; border-radius: 5px;"><a href="https://www.linkedin.com/in/ovide-kuichua">https://www.linkedin.com/in/ovide-kuichua</a></span>
</div>