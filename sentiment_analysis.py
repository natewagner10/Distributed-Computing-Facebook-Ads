positive = sc.textFile("/home/nate/positive-and-negative-words/positive-words.txt")
negative = sc.textFile("/home/nate/positive-and-negative-words/negative-words.txt")

positive_list = positive.collect()
negative_list = negative.collect()

#messages_trump = remove_tags.filter(lambda x: x[18] == 'Donald J. Trump for President, Inc.')



def mapper(line):
    return line[4]
    
#messages = messages_trump.map(mapper)


import nltk
nltk.download('stopwords')
stop_words = stopwords.words("english")

def removeStopWords(line):
    clean = []
    for word in line[4].split():
        word = word.lower()
        word = word.replace('.', '')
        word = word.replace('[^\w\s]'," ")
        word = word.replace("_"," ")
        if word not in (stop_words):
            clean.append(word)
        else:
            continue
    return line[0], line[1], line[2], line[3], line[4], clean, line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]  


messages_clean = messages.map(removeStopWords)


def posOrneg(line):
    pos = 0
    neg = 0
    for word in line[5]:
        if word in positive_list:
            pos += 1
        if word in negative_list:
            neg += 1
    if pos > neg:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], "positive"
    if pos < neg:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], "negative"
    else:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], "neutral"
        
        
        
        
total_sent = remove_tags.map(removeStopWords).map(posOrneg)

def filter_rdd(line):
    return line[4], line[19], line[20]
       
total_sent_df = total_sent.map(filter_rdd).toDF().selectExpr("_1 as message", "_2 as paid_for_by", "_3 as sentiment")
total_sent_df.createOrReplaceTempView("sent")
query = sqlContext.sql("select paid_for_by, sentiment, count(sentiment) cnt from sent group by paid_for_by, sentiment")
query.toPandas().to_csv('total_sentiment.csv')



#### ignore ####

messages_pos_neg = messages_clean.map(posOrneg) 
messages_pos_neg_df = messages_pos_neg.toDF()      
messages_pos_neg_df = messages_pos_neg_df.selectExpr("_1 as message", "_2 as sentiment")
messages_pos_neg_df.createOrReplaceTempView("sent")
cnt = sqlContext.sql("select distinct(sentiment), count(sentiment) from sent group by sentiment")


messages_bernie = remove_tags.filter(lambda x: x[18] == 'Bernie 2020.')
messages_bernie1 = messages_bernie.map(mapper).map(removeStopWords).map(posOrneg)
messages_bernie1_df = messages_bernie1.toDF().selectExpr("_1 as message", "_2 as sentiment")
messages_bernie1_df.createOrReplaceTempView("sent_bern")
cnt_bern = sqlContext.sql("select distinct(sentiment), count(sentiment) from sent_bern group by sentiment")

messages_need = remove_tags.filter(lambda x: x[18] == 'Need to Impeach')
messages_need1 = messages_need.map(mapper).map(removeStopWords).map(posOrneg)
messages_need1_df = messages_need1.toDF().selectExpr("_1 as message", "_2 as sentiment")
messages_need1_df.createOrReplaceTempView("sent_need")
cnt_need = sqlContext.sql("select distinct(sentiment), count(sentiment) from sent_need group by sentiment")



messages = remove_tags.map(mapper).map(removeStopWords).map(posOrneg)
messages_df = messages.toDF().selectExpr("_1 as message", "_2 as sentiment")
messages_df.createOrReplaceTempView("sent")
cnt_need = sqlContext.sql("select distinct(sentiment), count(sentiment) from sent group by sentiment")


def getSent(line):
    



