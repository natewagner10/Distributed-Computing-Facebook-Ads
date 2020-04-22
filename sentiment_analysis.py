positive = sc.textFile("/home/nate/positive-and-negative-words/positive-words.txt")
negative = sc.textFile("/home/nate/positive-and-negative-words/negative-words.txt")

positive_list = positive.collect()
negative_list = negative.collect()

messages_trump = remove_tags.filter(lambda x: x[18] == 'Donald J. Trump for President, Inc.')

def mapper(line):
    return line[4]
    
messages = messages_trump.map(mapper)


import nltk
nltk.download('stopwords')
stop_words = stopwords.words("english")

def removeStopWords(line):
    clean = []
    for word in line.split():
        word = word.lower()
        word = word.replace('.', '')
        word = word.replace('[^\w\s]'," ")
        word = word.replace("_"," ")
        if word not in (stop_words):
            clean.append(word)
        else:
            continue
    return clean  


messages_clean = messages.map(removeStopWords)


def posOrneg(line):
    pos = 0
    neg = 0
    for word in line:
        if word in positive_list:
            pos += 1
        if word in negative_list:
            neg += 1
    if pos > neg:
        return line, "positive"
    if pos < neg:
        return line, "negative"
    else:
        return line, "neutral"
        
        
messages_pos_neg = messages_clean.map(posOrneg) 
   
        









