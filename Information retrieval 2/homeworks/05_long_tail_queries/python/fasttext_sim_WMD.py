from nltk.corpus import stopwords
from nltk import download
import gensim
from tqdm import tqdm
from collections import defaultdict
from pymystem3 import Mystem
import sys

query_url_filename = sys.argv[1]
queries_filename = sys.argv[2]
model_rusvec_181_filename = sys.argv[3]
model_rusvec_187_filename = sys.argv[4]
model_ft_ru_filename = sys.argv[5]
model_ft_en_filename = sys.argv[6]
model_ft_deeppavlov_filename = sys.argv[7]
titles_filename = sys.argv[8]
features_filename = sys.argv[9]
lem_titles_filename = sys.argv[10]
lem_features_filename = sys.argv[11]

url_queries_map = defaultdict(list)
with open(query_url_filename) as query_url_f:
    for line in query_url_f:
        t = line[:-1].split('\t')
        query_id = t[0]
        url_id = t[1]
        url_queries_map[url_id].append(query_id)

query_map = {}
lem_query_map = {}
with open(queries_filename) as query_f:
    stem = Mystem()
    for line in tqdm(query_f, total=6311):
        t = line[:-1].split('\t')
        query_id = t[0]
        query = t[1].strip().lower()
        lem_query = ''.join(stem.lemmatize(query)[:-1])
        query_map[query_id] = query
        lem_query_map[query_id] = lem_query

model_rusvec_181 = gensim.models.KeyedVectors.load('181/model.model')
model_rusvec_187 = gensim.models.KeyedVectors.load("187/model.model")
model_ft_ru = gensim.models.fasttext.load_facebook_model('cc.ru.300.bin')
model_ft_en = gensim.models.fasttext.load_facebook_model('cc.en.300.bin')
model_ft_deeppavlov = gensim.models.fasttext.load_facebook_model('ft_native_300_ru_wiki_lenta_lemmatize.bin')
download('stopwords')
stop_words = set(stopwords.words('russian'))

with open('titles.tsv') as docs_f, open('title_fasttext_sim_WMD.tsv', 'w') as features_f:
    for line in tqdm(docs_f, total=582167):
        t = line[:-1].split('\t')
        url_id = t[0]
        if len(t) == 1:
            continue
        if url_id in url_queries_map:
            for query_id in url_queries_map[url_id]:
                query_words = query_map[query_id].split(' ')
                query_words = [w for w in query_words if w not in stop_words]
                title_words = t[1].split(' ')
                title_words = [w for w in title_words if w not in stop_words]

                if len(title_words) == 0 or len(query_words) == 0:
                    title_sim_rusvec_181 = 0
                    title_wmd_rusvec_181 = 1000000
                    title_sim_rusvec_187 = 0
                    title_wmd_rusvec_187 = 1000000
                    title_sim_ft_ru = 0
                    title_wmd_ft_ru = 1000000
                    title_sim_ft_en = 0
                    title_wmd_ft_en = 1000000
                    title_sim_ft_deeppavlov = 0
                    title_wmd_ft_deeppavlov = 1000000
                else:
                    title_sim_rusvec_181 = model_rusvec_181.n_similarity(query_words, title_words)
                    title_wmd_rusvec_181 = model_rusvec_181.wmdistance(query_words, title_words)
                    title_sim_rusvec_187 = model_rusvec_187.n_similarity(query_words, title_words)
                    title_wmd_rusvec_187 = model_rusvec_187.wmdistance(query_words, title_words)
                    title_sim_ft_ru = model_ft_ru.n_similarity(query_words, title_words)
                    title_wmd_ft_ru = model_ft_ru.wmdistance(query_words, title_words)
                    title_sim_ft_en = model_ft_en.n_similarity(query_words, title_words)
                    title_wmd_ft_en = model_ft_en.wmdistance(query_words, title_words)
                    title_sim_ft_deeppavlov = model_ft_deeppavlov.n_similarity(query_words, title_words)
                    title_wmd_ft_deeppavlov = model_ft_deeppavlov.wmdistance(query_words, title_words)

                res = query_id + '\t' + url_id + '\t' + \
                      str(title_sim_rusvec_181) + '\t' + str(title_wmd_rusvec_181) + '\t' + \
                      str(title_sim_rusvec_187) + '\t' + str(title_wmd_rusvec_187) + '\t' + \
                      str(title_sim_ft_ru) + '\t' + str(title_wmd_ft_ru) + '\t' + \
                      str(title_sim_ft_en) + '\t' + str(title_wmd_ft_en) + '\t' + \
                      str(title_sim_ft_deeppavlov) + '\t' + str(title_wmd_ft_deeppavlov) + \
                      '\n'
                features_f.write(res)

with open('lem_titles.tsv') as docs_f, open('lem_title_fasttext_sim_WMD.tsv', 'w') as features_f:
    for line in tqdm(docs_f, total=582167):
        t = line[:-1].split('\t')
        url_id = t[0]
        if len(t) == 1:
            continue
        if url_id in url_queries_map:
            for query_id in url_queries_map[url_id]:
                query_words = lem_query_map[query_id].split(' ')
                query_words = [w for w in query_words if w not in stop_words]
                title_words = t[1].split(' ')
                title_words = [w for w in title_words if w not in stop_words]

                if len(title_words) == 0 or len(query_words) == 0:
                    title_sim_rusvec_181 = 0
                    title_wmd_rusvec_181 = 1000000
                    title_sim_rusvec_187 = 0
                    title_wmd_rusvec_187 = 1000000
                    title_sim_ft_ru = 0
                    title_wmd_ft_ru = 1000000
                    title_sim_ft_en = 0
                    title_wmd_ft_en = 1000000
                    title_sim_ft_deeppavlov = 0
                    title_wmd_ft_deeppavlov = 1000000
                else:
                    title_sim_rusvec_181 = model_rusvec_181.n_similarity(query_words, title_words)
                    title_wmd_rusvec_181 = model_rusvec_181.wmdistance(query_words, title_words)
                    title_sim_rusvec_187 = model_rusvec_187.n_similarity(query_words, title_words)
                    title_wmd_rusvec_187 = model_rusvec_187.wmdistance(query_words, title_words)
                    title_sim_ft_ru = model_ft_ru.n_similarity(query_words, title_words)
                    title_wmd_ft_ru = model_ft_ru.wmdistance(query_words, title_words)
                    title_sim_ft_en = model_ft_en.n_similarity(query_words, title_words)
                    title_wmd_ft_en = model_ft_en.wmdistance(query_words, title_words)
                    title_sim_ft_deeppavlov = model_ft_deeppavlov.n_similarity(query_words, title_words)
                    title_wmd_ft_deeppavlov = model_ft_deeppavlov.wmdistance(query_words, title_words)

                res = query_id + '\t' + url_id + '\t' + \
                      str(title_sim_rusvec_181) + '\t' + str(title_wmd_rusvec_181) + '\t' + \
                      str(title_sim_rusvec_187) + '\t' + str(title_wmd_rusvec_187) + '\t' + \
                      str(title_sim_ft_ru) + '\t' + str(title_wmd_ft_ru) + '\t' + \
                      str(title_sim_ft_en) + '\t' + str(title_wmd_ft_en) + '\t' + \
                      str(title_sim_ft_deeppavlov) + '\t' + str(title_wmd_ft_deeppavlov) + \
                      '\n'
                features_f.write(res)
