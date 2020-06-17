from pymystem3 import Mystem
import re
from tqdm import tqdm
import sys

docs_filename = sys.argv[1]
titles_filename = sys.argv[2]
lem_titles_filename = sys.argv[3]
full_filename = sys.argv[4]
lem_full_filename = sys.argv[5]

mystem = Mystem()
SPLIT_RGX = re.compile(r'[A-Za-zА-Яа-я0-9]+')

def split(string):
    words = re.findall(SPLIT_RGX, string)
    return words


docs_num = 582167

with open(docs_filename, 'r') as docs_file, open(titles_filename, 'w') as titles_file, open(lem_titles_filename, 'w') as lem_titles_file, \
        open(full_filename, 'w') as full_file, open(lem_full_filename, 'w') as lem_full_file:
    cache_dict = {}
    for line in tqdm(docs_file, total=docs_num):
        t = line[:-1].strip().lower().split('\t')
        doc_id = t[0]
        if len(t) > 1:
            title = t[1]
        if len(t) > 2:
            body = t[2]

        if len(t) > 1:
            lem_title = []
            for word in split(title):
                if word in cache_dict:
                    lem_title.append(cache_dict[word])
                else:
                    a = mystem.lemmatize(word)[0]
                    lem_title.append(a)
                    cache_dict[word] = a

        if len(t) > 2:
            lem_body = []
            for word in split(body):
                if word in cache_dict:
                    lem_body.append(cache_dict[word])
                else:
                    a = mystem.lemmatize(word)[0]
                    lem_body.append(a)
                    cache_dict[word] = a

        title_res = doc_id
        lem_title_res = doc_id
        full_res = doc_id
        lem_full_res = doc_id
        if len(t) > 1:
            title_res += '\t'
            title_res += title
            lem_title_res += '\t'
            lem_title_res += ' '.join(lem_title)
            full_res += '\t'
            full_res += title
            lem_full_res += '\t'
            lem_full_res += ' '.join(lem_title)
        if len(t) > 2:
            full_res += '\t'
            full_res += body
            lem_full_res += '\t'
            lem_full_res += ' '.join(lem_body)
        title_res += '\n'
        lem_title_res += '\n'
        full_res += '\n'
        lem_full_res += '\n'

        titles_file.write(title_res)
        lem_titles_file.write(lem_title_res)
        full_file.write(full_res)
        lem_full_file.write(lem_full_res)
