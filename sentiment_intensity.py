import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()


def posOrneg(line):
    sentiment = sid.polarity_scores(line[4])
    neg = sentiment['neg']
    neu = sentiment['neu']
    pos = sentiment['pos']
    overall = ''
    if neg > neu and neg > pos:
        overall = 'negative'
    if neu > neg and neu > pos":
        overall = 'neutral'
    if pos > neg and pos > neu:
        overall = 'positive'
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], overall, sentiment['compound']
    

def posOrneg(line):
    sentiment = sid.polarity_scores(line[4])
    neg = sentiment['neg']
    neu = sentiment['neu']
    pos = sentiment['pos']
    overall = ''
    if neg > neu and neg > pos:
        overall = 'negative'
    if neu > neg and neu > pos":
        overall = 'neutral'
    if pos > neg and pos > neu:
        overall = 'positive'
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], overall, sentiment['compound']

















