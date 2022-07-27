# coding: utf-8

"""
Note: Le multithreading est utilisé pour decomposer le travail et le repartir sur les differents procésseur.
Cela contribu à l'optimisation de la vitesse de traitement des données. 
Et empeche de faire bugger l'ordinateur car au lieu de faire une grosse tache l'ordinateur fait plusieurs petite taches.
"""

#%%
import os
import threading
from collections import defaultdict, namedtuple
from typing import DefaultDict, List


MAX_THREAD_START_NUMBER = 11
MAX_BATCH = 1000
INPUT_DATA_FOLDER = "data"
OUTPUT_DATA_FOLDER = "decoded"
LOCK = threading.RLock()

build_url = lambda url: f"https://ru.wikipedia.org/wiki/{url}"
##

#%%
#Creation du répertoire de sortie de données
if not os.path.exists(OUTPUT_DATA_FOLDER):
    os.makedirs(OUTPUT_DATA_FOLDER)
##

#%%
def make_lang_mapper(key:int)->(None):
    """
    Creer un table de mappage clé-valeur. Cette table de mappage permet de lister les differentes langue dans
    lesquels un article et traduits et son titre dans la version traduite.

    Args:
        key (int): The key of the given map who contains a MAX_BATCH line (for ex: 1000 lines of the input file).

    Returns:
        None
    """

    #Note la valeur est un dictionary dont les clés sont les codes iso correspondant à chaque langue et la valeur le titre de l'article
    # dans la langue traduite

    #Note: Cette approche permet de reduire la compléxité de fouille d'une langue de O(N) (où N est le nombre totale de langue existant 
    # sur wikipedia) à O(1). Cela permet de trouver si une traduction existe en 1 seule operation
    for line in list_strings[key]:
        with LOCK:
            ll_from, ll_lang, ll_title = line.strip().split("\t")
            ll_title = ll_title.strip().strip("'").strip().encode("utf-8").decode("utf8").strip()
            # Si le titre dans une langue correspond à une chaine de charactère vide alors la traduction dans cette langue n'existe pas
            if ll_title != "":
                pages_langs[int(ll_from)][ll_lang.strip("'")] = ll_title

    with LOCK:
        #Libération de l'espace occupé en mémoire par les n lignes traitées.
        list_strings[key].clear()
        list_strings.pop(key)


def find_right_fit_and_export(key:int):
    """
        Cette procédure permet de trouver si une article ne possède pas la traduction francaise ou francaise et anglaise

        Args:
            key (int): The key of the given map who contains a MAX_BATCH line (for ex: 1000 lines of the input file).

        Returns:
            None
        
    """
    for line in pages_descriptions[key]:
        with LOCK:
            page_id, namespace, title, is_redirect = line.strip().split("\t")
            if int(namespace) != 0 or int(is_redirect) != 0:
                continue
            
            #Les titres étant des chaines de caractères sous forme de bytes array il faut effectuer decodage customizé
            decoded_title = decode_text(b"{0}".format(title.strip().strip("'")))
            url = build_url(decoded_title)


            #Tester si l'article courant n'a pas de traduction en francais et en anglais si oui l'Enregistrez dans un fichier
            #sinon regarder s'il n'a pas de traduction en francais uniquement si oui l'enregistrer si non ne rien faire
            langs_set = pages_langs.get(int(page_id))
            if (langs_set == None) or (langs_set != None and (langs_set.get("en") in (None, "")) and (langs_set.get("fr") in (None, ""))):
                    print(f"{page_id}\t\t{decoded_title}\t\t{url}", file=output_without_fr_en_file)
            elif (langs_set != None and (langs_set.get("fr") in (None, ""))):
                    print(f"{page_id}\t\t{decoded_title}\t\t{url}", file=output_without_fr_file)
    with LOCK:
        #Libération de l'espace occupé en mémoire par les n lignes traitées.
        pages_descriptions[key].clear()
        pages_descriptions.pop(key)


def decode_text(encoded_text:str) -> (str):
        """
        Décodé le texte fournie en entrée et retourné le texte décodé en sortie.
        
        Args:
            encoded_text: le texte encodé.
        
        Returns: 
            Le texte décodé.
        """
    
        ExtractedString = namedtuple("ExtractedString", ["string", "index"])  

        #Comme les texts encode peuvent contenir à la fois des bytes array et des chaines 
        # de caractères qui ne sont pas décodable, il faut juste extraire chaque chaine
        # decodable et les concerver ainsi que leurs positions dans le texte original
        # pour les rajouter apres.
        
        start_pos = encoded_text.find("\\")
        if start_pos != -1:
            extractedstrings = []
            bytes_stack = []
            idx = 0

            if start_pos > 0:
                extractedstrings.append(ExtractedString(encoded_text[:start_pos], 0))
                idx = 1

            for byte_part in encoded_text[start_pos:].split("\\x"):
                try:
                    _ = int(byte_part, 16)
                    if len(byte_part) == 2:
                        bytes_stack.append(byte_part)
                    else:
                        raise Exception
                except:
                    flag = byte_part
                    try:
                        _ = int(byte_part[:2], 16)
                        bytes_stack.append(byte_part[:2])
                        flag = byte_part[2:]
                    except:
                        pass
                    extractedstrings.append(ExtractedString(flag, len(bytes_stack)//2 + idx))
            bytes_string = " ".join(bytes_stack)

            decoded_text_array = list(bytearray.fromhex(bytes_string).decode())

            #Replace extractedString in the decoded string
            counter = 0
            for extractedstring in extractedstrings:
                decoded_text_array.insert(extractedstring.index + counter, extractedstring.string)
                counter += 1
                
            #Merge the string in decoded array
            return "".join(decoded_text_array)
        return encoded_text



### 
#%% Ouverture du fichier de lang et construction de la table de mappage 
# des articles aux langues dans lesquels ils sont traduit


counter = 1
key = 0
list_strings:DefaultDict[int, List[str]] = defaultdict(list)
pages_langs = defaultdict(dict)

lang_file_name = "langlinks_encoded_version.csv"

lang_file = open(os.path.join(INPUT_DATA_FOLDER, lang_file_name), "rt", encoding="utf-8")

threads_alive = len(threading.enumerate())
print("openning lang_file")
print("Successfully openend")
print("Looping on entries")
for line in lang_file:
    list_strings[key].append(line)
    if counter % MAX_BATCH == 0:
        while len(threading.enumerate()) >= MAX_THREAD_START_NUMBER - 1:
            flag = 1
        th = threading.Thread(target=make_lang_mapper, args=(key,))
        th.daemon = True
        th.start()
        key += 1
    counter += 1
else:
    while len(threading.enumerate()) >= MAX_THREAD_START_NUMBER - 1:
        flag = 1
    th = threading.Thread(target=make_lang_mapper, args=(key,))
    th.daemon = True
    th.start()

print('wait ends of thread')
while len(threading.enumerate()) >= threads_alive:
    print(len(threading.enumerate()))
    flag = 1
lang_file.close()
print("Pages langs dictionary is successfull builded")
###
#%% Ouverture du fichier contenant les articles et extraction de ceux qui n'ont
# ni de traduction en france; ni de traduction en francais et en anglais


print("openning page file")

threads_alive = len(threading.enumerate())

page_file_name = "pages_encoded_version.csv"
output_without_fr_name = "database_without_articles_translate_in_fr.csv"
output_without_fr_en_name = "database_without_articles_translate_in_fr_and_en.csv"

key = 0
counter = 1
pages_descriptions:DefaultDict[int, List[str]] = defaultdict(list)

page_file = open(os.path.join(INPUT_DATA_FOLDER, page_file_name), "rt", encoding="utf-8")
output_without_fr_file = open(os.path.join(OUTPUT_DATA_FOLDER, output_without_fr_name), "wt", encoding="utf-8")
output_without_fr_en_file = open(os.path.join(OUTPUT_DATA_FOLDER,output_without_fr_en_name), "wt", encoding="utf-8")
    

print("Successfully openned")

print("page_ids\t\tpage_titles\t\turl", file=output_without_fr_file)
print("page_ids\t\tpage_titles\t\turl", file=output_without_fr_en_file)

print("Looping on entries and insert data in ouput file")
for line in page_file:
    pages_descriptions[key].append(line)
    if counter % MAX_BATCH == 0:
        while len(threading.enumerate()) >= MAX_THREAD_START_NUMBER - 1:
            flag = 1
        th = threading.Thread(target=find_right_fit_and_export, args=(key,))
        th.daemon = True
        th.start()
        key += 1
    counter += 1
else:
    while len(threading.enumerate()) >= MAX_THREAD_START_NUMBER - 1:
        flag = 1
    th = threading.Thread(target=find_right_fit_and_export, args=(key,))
    th.daemon = True
    th.start()

print('wait ends of thread')
while len(threading.enumerate()) > threads_alive:
    print(len(threading.enumerate()))
    flag = 1
    
page_file.close()
output_without_fr_file.close()
output_without_fr_en_file.close()

print("Merging is end")
print("End of Processing")