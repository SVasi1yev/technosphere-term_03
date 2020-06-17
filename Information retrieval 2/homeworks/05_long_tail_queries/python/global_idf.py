from collections import defaultdict
from tqdm import tqdm
from math import log2
import json
import sys

full_filename = sys.argv[1]
global_idf_filename = sys.argv[2]
lem_full_filename = sys.argv[3]
global_lem_idf_filename = sys.argv[4]

docs_num = 582167

idf = defaultdict(int)
with open(lem_full_filename, 'r') as docs_file:
    for line in tqdm(docs_file, total=docs_num):
        t = ' '.join(line[:-1].split('\t')[1:])
        proc_words = set()
        for word in t.split(' '):
            if word not in proc_words:
                idf[word] += 1
                proc_words.add(word)

res = {}
for word in tqdm(idf):
    res[word] = log2(docs_num / idf[word])
with open(global_lem_idf_filename, 'w') as idf_file:
    json.dump(res, idf_file)


idf = defaultdict(int)
with open(full_filename, 'r') as docs_file:
    for line in tqdm(docs_file, total=docs_num):
        t = ' '.join(line[:-1].lower().split('\t')[1:])
        proc_words = set()
        for word in t.split(' '):
            if word not in proc_words:
                idf[word] += 1
                proc_words.add(word)

res = {}
for word in tqdm(idf):
    res[word] = log2(docs_num / idf[word])
with open(global_idf_filename, 'w') as idf_file:
    json.dump(res, idf_file)