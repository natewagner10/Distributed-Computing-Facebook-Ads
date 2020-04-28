from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
stop_words = stopwords.words("english")

def removeStopWords(line):
    clean = []
    try:
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
    except:
        return line[0], line[1], line[2], line[3], line[4], "didnt work", line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]  

messages_clean = remove_tags.map(removeStopWords)

imm_wb = ["immigration", "muslim", "border", "wall", "entering the country", "aliens", "deporting", 
"sanctuary", "illegal immigrants", "assimilation", "border security", "citizenship", "visas", "visa"]

def classify_immigrant(line):
    try:
        cnt = 0
        for word in line[5]:
            if word in imm_wb:
                cnt += 1           
        if cnt != 0:
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'immigration'
        else:
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other'
    except:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other'


data_w_imm = messages_clean.map(classify_immigrant)






