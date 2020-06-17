# Запуск mapred:
2017 - распакованный 2017.tar  
Остальные файлы находятся в папке meta_data  
hadoop jar 05_long_tail_queries.jar DocDocQueryStatsJob 2017 url_features url.data queries.tsv train.marks.tsv sample.csv url  
Полученные файлы переименовать и поместить в папку: url_features/global_features.tsv, url_features/train_query_features.tsv, url_features/test_query_features.tsv  
hadoop jar 05_long_tail_queries.jar DocDocQueryStatsJob 2017 host_features host.data queries.tsv train_host.tsv test_host.csv host  
Полученные файлы переименовать и поместить в папку: host_features/global_features.tsv, host_features/query_features.tsv

# lemmatizer.py
python lemmatizer.py docs.tsv text_data/titles.tsv text_data/lem_titles.tsv text_data/full.tsv text_data/lem_full.tsv  

# global_idf.py
python global_idf.py text_data/full.tsv idf/global_idf.json text_data/lem_full.tsv idf/global_lem_idf.json  

# tf_idf_bm25.py  
python tf_idf_bm25.py meta_data/query_url.tsv meta_data/queries.tsv idf/global_lem_idf.json lem_docs.tsv tf_idf_bm25.tsv  

# fasttext_sim_WMD.py
Модели:  
1. http://vectors.nlpl.eu/repository/20/181.zip
2. http://vectors.nlpl.eu/repository/20/187.zip  
3. https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.ru.300.bin.gz
4. https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.bin.gz
5. http://files.deeppavlov.ai/embeddings/ft_native_300_ru_wiki_lenta_lemmatize/ft_native_300_ru_wiki_lenta_lemmatize.bin  
python fasttext_sim_WMD.py meta_data/query_url.tsv meta_data/queries.tsv 181/model.model 187/model.model cc.ru.300.bin cc.en.300.bin ft_native_300_ru_wiki_lenta_lemmatize.bin text_data/titles.tsv text_features/title_fasttext_sim_WMD.tsv text_data/lem_titles.tsv text_features/lem_title_fasttext_sim_WMD.tsv
 
# bert.py  
Модели:  
1. http://files.deeppavlov.ai/deeppavlov_data/bert/sentence_ru_cased_L-12_H-768_A-12.tar.gz  
2. http://files.deeppavlov.ai/deeppavlov_data/bert/sentence_multi_cased_L-12_H-768_A-12.tar.gz  
python bert.py meta_data/query_url.tsv meta_data/queries.tsv <папка модели 1> <папка модели 2> text_data/titles.tsv text_data/lem_titles.tsv bert_cosine.tsv

# predict.ipynb
Запустить все ячейки, получить файл sub.txt
