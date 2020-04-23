import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()


def posOrneg(line):
    sentiment = sid.polarity_scores(line[4])
    neg = sentiment['neg']
    neu = sentiment['neu']
    pos = sentiment['pos']
    if neg > neu and neg > pos:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], neg, neu, pos, "negative", sentiment['compound']
    if neu > neg and neu > pos:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], neg, neu, pos, "neutral", sentiment['compound']
    if pos > neg and pos > neu:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], neg, neu, pos, "positive", sentiment['compound']
    else:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], neg, neu, pos, "neutral", sentiment['compound']
    


total_sent = target.map(posOrneg)

def filter_rdd(line):
    return line[4], line[18], line[20], line[21], line[22], line[23], line[24]

total_sent_df = total_sent.map(filter_rdd).toDF().selectExpr("_1 as message", "_2 as paid_for_by", "_3 as neg", "_4 as neu", "_5 as pos", "_6 as sentiment", "_7 as score")
total_sent_df.show(5)
total_sent_df.createOrReplaceTempView("sent")


query = sqlContext.sql("select paid_for_by, sentiment, count(sentiment) cnt from sent group by paid_for_by, sentiment")
#query.toPandas().to_csv('total_sentiment.csv')




