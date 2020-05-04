import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()


def posOrneg(line):
    sentiment = sid.polarity_scores(line[4])
    neg = sentiment['neg']
    neu = sentiment['neu']
    pos = sentiment['pos']
    if neg > neu and neg > pos:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], line[21], neg, neu, pos, "negative", sentiment['compound']
    if neu > neg and neu > pos:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], line[21],  neg, neu, pos, "neutral", sentiment['compound']
    if pos > neg and pos > neu:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], line[21],  neg, neu, pos, "positive", sentiment['compound']
    else:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], line[21],  neg, neu, pos, "neutral", sentiment['compound']
    


total_sent = data_w_imm.map(posOrneg)

def filter_rdd(line):
    return line[4], line[19], line[21], line[22], line[23], line[24], line[25], line[7].month, line[7].day, line[7].year

def createSome(line):
    if line[6] == "neutral":
        if line[3] >= 0.15:
            return line[0], line[1], line[2], line[3], line[4], line[5], "somewhat negative", line[7], line[8], line[9]
        if line[5] >= 0.15:
            return line[0], line[1], line[2], line[3], line[4], line[5], "somewhat positive", line[7], line[8], line[9]
        else:
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9]
    else:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9]
        


total_sent_feed = total_sent.map(filter_rdd).map(createSome)

reduced_data = total_sent_feed.filter(lambda x: x[2] != 'other')
      
        
reduced_data_df = reduced_data.toDF().selectExpr("_1 as message", "_2 as paid_for_by", "_3 as category", "_4 as neg", "_5 as neut", "_6 as pos", "_7 as sentiment", "_8 as month", "_9 as day", "_10 as year")
reduced_data_df.show(10)
total_sent_df.createOrReplaceTempView("sent")

query = sqlContext.sql("select sentiment, count(sentiment) cnt from sent group by sentiment")
query = sqlContext.sql("select paid_for_by, sentiment, count(sentiment) cnt from sent group by paid_for_by, sentiment")
#query.toPandas().to_csv('total_sentiment.csv')




