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
            word = word.replace('-', '')
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

health_wb = [ 'pre existing conditions', 'drug price', 'drug prices', 
             'drug price regulation', 'live saving drug', 'life saving drugs', 
             'healthcare', 'mental health', 'obamacare', 'medicaid', 'single payer healthcare', 
             'marijuna', 'safe haven', 'va privatization' ]
econ_wb = ['globalization', 'international relations', 'trade', 'finance',
           'financial crisis', 'bond market', 'commodity markets', 'financial contagion',
           'financial market', 'securities', 'security markets', 'share market',
           'stock market', 'taxation', 'tax', 'taxes', 'raise taxes', 'anti poverty',
           'poverty', 'welfare', 'employment', 'employment creation', 'unemployment',
           'unemployment rate', 'hires', 'job creation', 'wage', 'minimum wage',
           'paid sick leave', 'corporate tax', 'capital gains', 'capital gains tax',
           'compensation', 'labor costs', 'wages', 'wage data', 'union', 'nonunion', 'accounting',
           'Business Administration', 'business Economics', 'marketing']

def classify_immigrant(line):
    try:
        cnt_imm = 0
        cnt_health = 0
        cnt_econ = 0
        for word in line[5]:
            if word in imm_wb:
                cnt_imm += 1
            if word in health_wb:
                cnt_health += 1
            if word in econ_wb:
                cnt_econ += 1
        if cnt_imm != 0 or cnt_health != 0 or cnt_econ != 0:
            if cnt_imm > cnt_health and cnt_imm > cnt_econ:
                return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'immigration'    
            elif cnt_health > cnt_imm and cnt_health > cnt_econ:
                return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'healthcare'
            elif cnt_econ > cnt_imm and cnt_econ > cnt_health:
                return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'economic'
            else:
                return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other'
        else:
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other'
    except:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other'


data_w_imm = messages_clean.map(classify_immigrant)
data_w_imm.filter(lambda x: x[21] == 'immigration').count()





