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
    return line[19], line[21], line[22], line[23], line[24], line[25], line[7].month, line[7].day, line[7].year, line[14]

def createSome(line):
    if line[5] == "neutral":
        if line[2] >= 0.15:
            return line[0], line[1], line[2], line[3], line[4], "somewhat negative", line[6], line[7], line[8], line[9]
        elif line[4] >= 0.15:
            return line[0], line[1], line[2], line[3], line[4], "somewhat positive", line[6], line[7], line[8], line[9]
        else:
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9]
    else:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9]
        


total_sent_feed = total_sent.map(filter_rdd).map(createSome)

reduced_data = total_sent_feed.filter(lambda x: x[1] != 'other')


      
        
reduced_data_df = reduced_data.toDF().selectExpr("_1 as paid_for_by", "_2 as category", "_3 as neg", "_4 as neut", "_5 as pos", "_6 as sentiment", "_7 as month", "_8 as day", "_9 as year", "_10 as target")
reduced_data_df.show(10)
total_sent_df.createOrReplaceTempView("sent")

query = sqlContext.sql("select sentiment, count(sentiment) cnt from sent group by sentiment")
query = sqlContext.sql("select paid_for_by, sentiment, count(sentiment) cnt from sent group by paid_for_by, sentiment")
#query.toPandas().to_csv('total_sentiment.csv')



train_x_new = []
fixed = 0
fixed_vec = []
cnt = 1
for i in train_x:
    try:
        for x in i:       
            x = x.replace("(", "")
            x = x.replace(")", "")
            x = x.replace("]", "")
            x = x.replace("[", "")
            if cnt == 1:
                print(x)
                fixed = float(x)
                cnt += 1
            else:
                fixed_vec.append(float(x))
        train_x_new = train_x_new.append((fixed, fixed_vec))
        print(fixed, fixed_vec)
        fixed = 0
        fixed_vec = []
    except:
        continue


