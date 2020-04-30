import pandas as pd
import numpy as np
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.preprocessing import LabelEncoder
from collections import defaultdict
from nltk.corpus import wordnet as wn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import model_selection, naive_bayes, svm
from sklearn.metrics import accuracy_score

def cleanWords(line):
    message = line[4].lower()
    message = word_tokenize(message)
    
    
    tag_map = defaultdict(lambda : wn.NOUN)
    tag_map['J'] = wn.ADJ
    tag_map['V'] = wn.VERB
    tag_map['R'] = wn.ADV
    Final_words = []
    # Initializing WordNetLemmatizer()
    word_Lemmatized = WordNetLemmatizer()
    # pos_tag function below will provide the 'tag' i.e if the word is Noun(N) or Verb(V) or something else.
    for word, tag in pos_tag(message):
        # Below condition is to check for Stop words and consider only alphabets
        if word not in stopwords.words('english') and word.isalpha():
            word_Final = word_Lemmatized.lemmatize(word,tag_map[tag[0]])
            Final_words.append(word_Final)
    # The final processed set of words for each iteration will be stored in 'text_final'
    fw = Final_words
    return (fw, line[21])

def cleaner(line):
    m = line[0]
    m = m.replace('[', '')
    m = m.replace(']', '')
    m = m.split(' ')
    new_vec = []
    for i in m:
        i = i.replace("'", "")
        i = i.replace(",", "")
        new_vec.append(i)
    return new_vec, line[1]
    

    
ads_clean_words = data_w_imm.map(cleanWords)#.map(cleaner)
soc_econ = ads_clean_words.filter(lambda x: x[1] == 'immigration' or x[1] == 'healthcare')



#def shrinker(line):
#    return line[0]
#test1 = soc_econ_encoded.map(shrinker)
#hashingTF = HashingTF()
#tf = hashingTF.transform(documents)
#tf.cache()
#idf = IDF().fit(tf)
#tfidf = idf.transform(tf)



from pyspark.ml.feature import CountVectorizer
soc_econ_df = soc_econ.toDF().selectExpr("_1 as message", "_2 as category")

from pyspark.ml.feature import Word2Vec
wordVec = Word2Vec(vectorSize=17530, minCount=0, inputCol="message", outputCol="features")
model = wordVec.fit(soc_econ_df)
result = model.transform(soc_econ_df)



train, test = soc_econ_df.randomSplit([0.7, 0.3], seed = 2018)

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol = 'message', labelCol = 'category', maxIter=10)
lrModel = lr.fit(train)




