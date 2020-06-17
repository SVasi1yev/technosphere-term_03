from deeppavlov.core.common.file import read_json
from deeppavlov import build_model, configs
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cdist
from tqdm import tqdm
from collections import defaultdict
from pymystem3 import Mystem
import sys

query_url_filename = sys.argv[1]
queries_filename = sys.argv[2]
model_dp_rus_dirname = sys.argv[3]
model_dp_mult_dirname = sys.argv[4]
titles_filename = sys.argv[5]
lem_titles_filename = sys.argv[6]
features_filename = sys.argv[7]
lem_features_filename = sys.argv[8]

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

doc_num = 582167

bert_config_rus = read_json(configs.embedder.bert_embedder)
bert_config_rus['metadata']['variables']['BERT_PATH'] = model_dp_rus_dirname
model_dp_rus = build_model(bert_config_rus)

bert_config_mult = read_json(configs.embedder.bert_embedder)
bert_config_mult['metadata']['variables']['BERT_PATH'] = model_dp_mult_dirname
model_dp_mult = build_model(bert_config_mult)

model_sent_trans = SentenceTransformer('distiluse-base-multilingual-cased')

with open(titles_filename) as docs_f, open(lem_titles_filename) as lem_doc_f, open(features_filename, 'w') as features_f:
    for line in tqdm(docs_f, total=doc_num):
        t = line[:-1].split('\t')
        lem_t = lem_doc_f.readline()[:-1].split('\t')
        url_id = t[0]
        if len(t) == 1:
            continue
        title = [t[1]]
        lem_title = [lem_t[1]]
        if url_id in url_queries_map:
            for query_id in url_queries_map[url_id]:
                query = [query_map[query_id]]
                title_tokens, title_token_embs, title_subtokens, title_subtoken_embs, \
                title_sent_max_embs, title_sent_mean_embs, title_bert_pooler_outputs = model_dp_rus(title)
                query_tokens, query_token_embs, query_subtokens, query_subtoken_embs, \
                query_sent_max_embs, query_sent_mean_embs, query_bert_pooler_outputs = model_dp_rus(query)
                rus_cos = cdist([title_sent_mean_embs.reshape(-1)], [query_sent_mean_embs.reshape(-1)], 'cosine')

                title_tokens, title_token_embs, title_subtokens, title_subtoken_embs, \
                title_sent_max_embs, title_sent_mean_embs, title_bert_pooler_outputs = model_dp_mult(title)
                query_tokens, query_token_embs, query_subtokens, query_subtoken_embs, \
                query_sent_max_embs, query_sent_mean_embs, query_bert_pooler_outputs = model_dp_mult(query)
                mult_cos = cdist([title_sent_mean_embs.reshape(-1)], [query_sent_mean_embs.reshape(-1)], 'cosine')

                title_sent_mean_embs = model_sent_trans.encode(title)
                query_sent_mean_embs = model_sent_trans.encode(query)
                sent_trans_cos = cdist(title_sent_mean_embs, query_sent_mean_embs, 'cosine')

                res = query_id + '\t' + url_id + '\t' + \
                      str(rus_cos) + '\t' + str(mult_cos) + '\t' + \
                      str(sent_trans_cos[0][0]) + '\t'

                lem_query = [lem_query_map[query_id]]
                title_tokens, title_token_embs, title_subtokens, title_subtoken_embs, \
                title_sent_max_embs, title_sent_mean_embs, title_bert_pooler_outputs = model_dp_rus(lem_title)
                query_tokens, query_token_embs, query_subtokens, query_subtoken_embs, \
                query_sent_max_embs, query_sent_mean_embs, query_bert_pooler_outputs = model_dp_rus(lem_query)
                rus_cos = cdist([title_sent_mean_embs.reshape(-1)], [query_sent_mean_embs.reshape(-1)], 'cosine')

                title_tokens, title_token_embs, title_subtokens, title_subtoken_embs, \
                title_sent_max_embs, title_sent_mean_embs, title_bert_pooler_outputs = model_dp_mult(lem_title)
                query_tokens, query_token_embs, query_subtokens, query_subtoken_embs, \
                query_sent_max_embs, query_sent_mean_embs, query_bert_pooler_outputs = model_dp_mult(lem_query)
                mult_cos = cdist([title_sent_mean_embs.reshape(-1)], [query_sent_mean_embs.reshape(-1)], 'cosine')

                title_sent_mean_embs = model_sent_trans.encode(lem_title)
                query_sent_mean_embs = model_sent_trans.encode(lem_query)
                sent_trans_cos = cdist(title_sent_mean_embs, query_sent_mean_embs, 'cosine')

                res += str(rus_cos) + '\t' + str(mult_cos) + '\t' + \
                      str(sent_trans_cos[0][0]) + '\n'

                features_f.write(res)
