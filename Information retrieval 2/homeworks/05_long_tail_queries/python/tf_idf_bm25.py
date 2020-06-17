import json
from tqdm import tqdm
from _collections import defaultdict
from pymystem3 import Mystem
import sys

query_url_filename = sys.argv[1]
queries_filename = sys.argv[2]
global_lem_idf_filename = sys.argv[3]
lem_docs_filename = sys.argv[4]
features_filename = sys.argv[5]

url_queries_map = defaultdict(list)
with open(query_url_filename) as query_url_f:
    for line in query_url_f:
        t = line[:-1].split('\t')
        query_id = t[0]
        url_id = t[1]
        url_queries_map[url_id].append(query_id)

query_map = {}
with open(queries_filename) as query_f:
    stem = Mystem()
    for line in tqdm(query_f, total=6311):
        t = line[:-1].split('\t')
        query_id = t[0]
        query = t[1].strip().lower()
        query = ''.join(stem.lemmatize(query)[:-1])
        query_map[query_id] = query

with open(global_lem_idf_filename) as idf_f:
    idf_map = json.load(idf_f)

with open(lem_docs_filename) as docs_f, open(features_filename, 'w') as features_f:
    title_num = 581519
    avg_title_len = 8.4
    full_num = 581229
    avg_full_len = 9765
    k = 2
    b = 0.75
    count = 0
    for line in tqdm(docs_f, total=582167):
        t = line[:-1].split('\t')
        url_id = t[0]
        if len(t) == 1:
            continue
        if url_id in url_queries_map:
            for query_id in url_queries_map[url_id]:
                count += 1
                query_words = query_map[query_id].split(' ')
                title_words = t[1].split(' ')
                full_words = title_words
                if len(t) == 3:
                    full_words = full_words + t[2].split(' ')

                title_tf = defaultdict(int)
                for word in title_words:
                    title_tf[word] += 1
                title_tf_idf = 0
                for word in query_words:
                    title_tf_idf += title_tf[word] * idf_map.get(word, 0) / len(title_words)

                full_tf = defaultdict(int)
                for word in full_words:
                    full_tf[word] += 1
                full_tf_idf = 0
                if len(t) == 3:
                    for word in query_words:
                        full_tf_idf += full_tf[word] * idf_map.get(word, 0) / len(full_words)

                title_bm25 = 0
                for word in query_words:
                    title_bm25 += idf_map.get(word, 0) * (title_tf[word] * (k + 1) / len(title_words)) / (title_tf[word] / len(title_words) + k * (1 - b + b * len(title_words) / avg_title_len))
                full_bm25 = 0
                if len(t) == 3:
                    for word in query_words:
                        full_bm25 += idf_map.get(word, 0) * (full_tf[word] * (k + 1) / len(full_words)) / (full_tf[word] / len(full_words) + k * (1 - b + b * len(full_words) / avg_full_len))

                res = query_id + '\t' + url_id + '\t' + str(title_tf_idf) + '\t' + str(full_tf_idf) + '\t' + str(title_bm25) + '\t' + str(full_bm25) + '\n'
                features_f.write(res)
