# coding: utf-8

import os
import re
import gzip
import sys
import threading

from typing import Generator, List, DefaultDict
from tqdm import tqdm 
from collections import defaultdict, namedtuple


MAX_THREAD_START_NUMBER = 11
MAX_BATCH = 1000
OUTPUT_DATA_FOLDER = "results"
LOCK = threading.RLock()
FILEPROPS=namedtuple("Fileprops", "parser num_fields column_indexes")

LANGLINKS_FILE_NAME = "ruwiki-latest-langlinks.sql.gz"
PAGES_FILE_NAME = "ruwiki-latest-page.sql.gz"


build_url = lambda url: f"https://ru.wikipedia.org/wiki/{url}"


global count_inserts
count_inserts = 0

#Creation du répertoire de sortie de données
if not os.path.exists(OUTPUT_DATA_FOLDER):
    os.makedirs(OUTPUT_DATA_FOLDER)

#CATEGORYLINKS_PARSER=re.compile(r'(?P<row0>[0-9]+?),(?P<row1>\'.*?\'?),(?P<row2>\'.*?\'?),(?P<row3>\'[0-9\ \-:]+\'?),(?P<row4>\'\'?),(?P<row5>\'.*?\'?),(?P<row6>\'.*?\'?)')
CATEGORYLINKS_PARSER=re.compile(r'^(?P<row0>[0-9]+?),(?P<row1>\'.*?\'?),(?P<row2>\'.*?\'?),(?P<row3>\'[0-9\ \-:]+\'?),(?P<row4>\'.*?\'?),(?P<row5>\'[a-z\-]*?\'?),(?P<row6>\'[a-z]+\'?)$')
PAGELINKS_PARSER=re.compile(r'^(?P<row0>[0-9]+?),(?P<row1>[0-9]+?),(?P<row2>\'.*?\'?),(?P<row3>[0-9]+?)$')
LANGLINKS_PARSER=re.compile(r'^(?P<row0>[0-9]+?),(?P<row1>\'.*?\'?),(?P<row2>\'.*?\'?)$')
REDIRECT_PARSER=re.compile(r'^(?P<row0>[0-9]+?),(?P<row1>-?[0-9]+?),(?P<row2>\'.*?\'?),(?P<row3>\'.*?\'?),(?P<row4>\'.*?\'?)$')
CATEGORY_PARSER=re.compile(r'^(?P<row0>[0-9]+?),(?P<row1>\'.*?\'?),(?P<row2>[0-9]+?),(?P<row3>[0-9]+?),(?P<row4>[0-9]+?)$')
PAGE_PROPS_PARSER=re.compile(r'^([0-9]+),(\'.*?\'),(\'.*?\'),(\'[0-9\ \-:]+\'),(\'\'),(\'.*?\'),(\'.*?\')$')
PAGE_PARSER=re.compile((r'^(?P<row0>[0-9]+?),(?P<row1>[0-9]+?),(?P<row2>\'.*?\'?),(?P<row3>[0-9]+?),(?P<row4>[0-9]?),'
    r'(?P<row5>[0-9\.]+?),(?P<row6>\'.*?\'?),(?P<row7>(?P<row7val>\'.*?\'?)|(?P<row7null>NULL)),(?P<row8>[0-9]+?),(?P<row9>[0-9]+?),'
    r'(?P<row10>(?P<row10val>\'.*?\'?)|(?P<row10null>NULL)),(?P<row11>(?P<row11val>\'.*?\'?)|(?P<row11null>NULL))$'))


"""
# page
`page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,
`page_namespace` int(11) NOT NULL DEFAULT 0,
`page_title` varbinary(255) NOT NULL DEFAULT '',
`page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT 0,
`page_is_new` tinyint(1) unsigned NOT NULL DEFAULT 0,
`page_random` double unsigned NOT NULL DEFAULT 0,
`page_touched` varbinary(14) NOT NULL,
`page_links_updated` varbinary(14) DEFAULT NULL,
`page_latest` int(8) unsigned NOT NULL DEFAULT 0,
`page_len` int(8) unsigned NOT NULL DEFAULT 0,
`page_content_model` varbinary(32) DEFAULT NULL,
`page_lang` varbinary(35) DEFAULT NULL,

#langlinks
`ll_from` int(8) unsigned NOT NULL DEFAULT 0,
`ll_lang` varbinary(35) NOT NULL DEFAULT '',
`ll_title` varbinary(255) NOT NULL DEFAULT '',

# pagelinks
`pl_from` int(8) unsigned NOT NULL DEFAULT '0',
`pl_namespace` int(11) NOT NULL DEFAULT '0',
`pl_title` varbinary(255) NOT NULL DEFAULT '',
`pl_from_namespace` int(11) NOT NULL DEFAULT '0',
"""


FILETYPE_PROPS=dict(
        categorylinks=FILEPROPS(CATEGORYLINKS_PARSER, 7, (0, 1, 6)),
        pagelinks=FILEPROPS(PAGELINKS_PARSER, 4, (0, 1, 2, 3)),
        langlinks=FILEPROPS(LANGLINKS_PARSER, 3, (0, 1, 2)),
        redirect=FILEPROPS(REDIRECT_PARSER, 5, (0, 1, 2)),
        category=FILEPROPS(CATEGORY_PARSER, 5, (0, 1, 2, 3, 4)),
        page_props=FILEPROPS(PAGE_PROPS_PARSER, 7, (0, 1)),
        page=FILEPROPS(PAGE_PARSER, 12, (0, 1, 2, 3, 9, 10, 11)),
        )

def parse_match(match, column_indexes) -> (Generator):
    """
        Cette fonction permet de tester si les données parsées correspondent
        au format spécifié par le parser 
        (ex: PAGE_PARSER sur les columns spécifiés dans page=FILEPROPS(PAGE_PARSER, 12, (0, 1, 2, 3, 9, 10, 11))) 
        auquel cas il retourne les colonnes spécifiées dans FILETYPE_PROPS correspondant au paser.
    """
    row = match.groupdict()
    return tuple(row["row{}".format(i)] for i in column_indexes)


def parse_value(value, parser, column_indexes, value_idx=0, pbar=None) -> (Generator):
    """
        Cette fonction extrait les données désirées (colonnes) de ligne fournie(en entrée)
        provenant du fichier de données (base de donnée sql). Et fournie en sortie
        un générateur (une list). 
        Elle remplace aussi les certains fragments de la ligne fournie par leurs equivalents
        en language humain c'est à dire le "-". Si une erreur de lors de l'extraction survient
        alors cette erreur est affiché à l'écran et le traitement continue 
    """

    #The origin of an error is from column type. If an error occur try to find if type of the column
    # is the same as the type of the correspondant column index in parser function.

    #The error may also comming from unmatching numbers of items(columns) in the given line 
    # against the numbers of items in parser or matcher

    # replace unicode dash with ascii dash
    value = value.replace("\\xe2\\x80\\x93", "-")
    parsed_correctly = False
    for _, match in enumerate(parser.finditer(value)):
        parsed_correctly = True
        try:
            row = parse_match(match, column_indexes)
            yield row
        except Exception as e:
            print("Line: {!r}, Exception: {}".format(value, e), file=sys.stderr)
    if not parsed_correctly:
        print("Line: {!r}, IDX: {}, Exception: {}".format(value, value_idx, "Unable to parse."), file=sys.stderr)


def process_insert_values_line(line, parser, column_indexes, count_inserts=0, pbar=None) -> (Generator):
    """
        Cette fonction extrait les colones désirées de la ligne courante de la base
        de données.
    """
    start, partition, values = line.partition(' VALUES ')
    # Each insert statement has format: 
    # INSERT INTO "table_name" VALUES (v1,v2,v3),(v1,v2,v3),(v1,v2,v3);
    # When splitting by "),(" we need to only consider string from values[1:-2]
    # This ignores the starting "(" and ending ");"
    values = values.strip()[1:-2].split("),(")
    pbar.set_postfix(found_values=len(values), insert_num=count_inserts)
    for value_idx, value in enumerate(values):
        for row in parse_value(value, parser, column_indexes, value_idx, pbar):
            yield row


def make_lang_mapper(key:int)->(None):
    """
    Creer un table de mappage clé-valeur. Cette table de mappage permet de lister les differentes langue dans
    lesquels un article et traduits et son titre dans la version traduite.

    Args:
        key (int): The key of the given map who contains a MAX_BATCH line (for ex: 1000 lines of the input file).

    Returns:
        None
    """

    #Les données issus de la base de données sont produites à la fonction(procédure) qui va extraire les parties interessant c'est à dire
    #ll_from, ll_lang et ll_title puis les utilisers pour le mappage

    #Note la valeur est un dictionary dont les clés sont les codes iso correspondant à chaque langue et la valeur le titre de l'article
    # dans la langue traduite

    #Note: Cette approche permet de reduire la compléxité de fouille d'une langue de O(N) (où N est le nombre totale de langue existant 
    # sur wikipedia) à O(1). Cela permet de trouver si une traduction existe en 1 seule operation
    
    global count_inserts
    
    for line in langlinks_file_lines[key]:
        with LOCK:
            if line.startswith('INSERT INTO `{}` VALUES '.format("langlinks")):
                count_inserts += 1
                for row in process_insert_values_line(
                        line, parser, column_indexes, count_inserts, pbar):
                    if pbar is not None:
                        pbar.update(1) 
                    ll_from, ll_lang, ll_title = row
                    ll_title = ll_title.strip().strip("'").strip().encode("utf-8").decode("utf8").strip()
                    # Si le titre dans une langue correspond à une chaine de charactère vide alors la traduction dans cette langue n'existe pas
                    if ll_title != "":
                        pages_langs[int(ll_from)][ll_lang.strip("'")] = ll_title

    with LOCK:
        #Libération de l'espace occupé en mémoire par les n lignes traitées.
        langlinks_file_lines[key].clear()
        langlinks_file_lines.pop(key)


def find_right_fit_and_export(key:int):
    """
        Cette procédure permet de trouver si une article ne possède pas la traduction francaise ou francaise et anglaise

        Args:
            key (int): The key of the given map who contains a MAX_BATCH line (for ex: 1000 lines of the input file).

        Returns:
            None
        
    """

    #Les données issus de la base de données sont produites à la fonction(procédure) qui va extraire les parties interessant c'est à dire
    #page_id, page_namespace, page_title, page_is_redirect et les fournir à l'algorithme afin qu'il trouve si cette article n'est pas une
    #redirection vers un autre mais aussi si cette article n'est ni en francais ou ni en francais et ni en anglais

    global count_inserts

    for line in pages_file_lines[key]:
        with LOCK:
            if line.startswith('INSERT INTO `{}` VALUES '.format("langlinks")):
                count_inserts += 1
                for row in process_insert_values_line(
                        line, parser, column_indexes, count_inserts, pbar):
                    if pbar is not None:
                        pbar.update(1) 

                    page_id, namespace, title, is_redirect = row
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
        pages_file_lines[key].clear()
        pages_file_lines.pop(key)


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


#Pour pouvoir faire du multithreading(creation de processus executables sur differents processeurs logiques) pour 
# accélérer le calcul de resultats; il faut decomposer le tache à effectuer en sous taches.
# L'idée ici est d'attribuer à chaque thread une certaines quantité de ligne à traiter. 
# Cette quantité est définie par MAX_BATCH et vaut 1000 par defaut.


counter = 1
key = 0
langlinks_file_lines:DefaultDict[int, List[str]] = defaultdict(list)
pages_langs = defaultdict(dict)

threads_alive = len(threading.enumerate())
with gzip.open(LANGLINKS_FILE_NAME, 'rt', encoding='ascii', errors='backslashreplace') as input_file:
    parser, _, ci = FILETYPE_PROPS["langlinks"]
    column_indexes = ci
    with tqdm(disable=None) as pbar:
        for _, line in enumerate(input_file, start=1):
            langlinks_file_lines[key].append(line)
            if  counter % MAX_BATCH == 0:
                while len(threading.enumerate()) >= MAX_THREAD_START_NUMBER - 1:
                    flag = 1
                th = threading.Thread(target=make_lang_mapper, args=(key,))
                th.daemon = True
                th.start()
            counter += 1
        else:
            while len(threading.enumerate()) >= MAX_THREAD_START_NUMBER - 1:
                flag = 1
            th = threading.Thread(target=make_lang_mapper, args=(key,))
            th.daemon = True
            th.start()
        
        print('Waiting for the end of work of all threads')
        while len(threading.enumerate()) >= threads_alive:
            flag = 1
        print("All threads end theirs works")

print("Pages langs dictionary is successfull builded")
        


print("\n\nStarting processing articles and saving results")

threads_alive = len(threading.enumerate())

output_without_fr_name = "database_without_articles_translate_in_fr.csv"
output_without_fr_en_name = "database_without_articles_translate_in_fr_and_en.csv"

key = 0
counter = 1
pages_file_lines:DefaultDict[int, List[str]] = defaultdict(list)

output_without_fr_file = open(os.path.join(OUTPUT_DATA_FOLDER, output_without_fr_name), "wt", encoding="utf-8")
output_without_fr_en_file = open(os.path.join(OUTPUT_DATA_FOLDER,output_without_fr_en_name), "wt", encoding="utf-8")

threads_alive = len(threading.enumerate())
with gzip.open(PAGES_FILE_NAME, 'rt', encoding='ascii', errors='backslashreplace') as input_file:
    parser, _, ci = FILETYPE_PROPS["page"]
    column_indexes = ci
    with tqdm(disable=None) as pbar:
        count_inserts = 0
        for _, line in enumerate(input_file, start=1):
            langlinks_file_lines[key].append(line)
            if  counter % MAX_BATCH == 0:
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

        print('Waiting for the end of work of all threads')
        while len(threading.enumerate()) >= threads_alive:
            flag = 1
        print("All threads end theirs works")

print("\nThe proccessing of data is end. Look the results file in the folder results")