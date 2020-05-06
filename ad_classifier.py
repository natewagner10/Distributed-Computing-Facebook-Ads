from csv import reader
import re 
import pandas as pd
import datetime
from datetime import date


my_df = spark.read.csv("/spring2020/data/fbpac-ads-en-US.csv", header = True, inferSchema = True, quote = "\"", escape = "\"")
df1 = my_df.drop('html')
df2 = df1.drop('targetings').drop('targeting').drop('targetedness')
df2.show(1)
                          
rdd = df2.rdd.map(tuple)
rdd.take(2)

def whatis(line):
  return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]

whatis = rdd.map(whatis)


#remove hyperfeed_story
#accounts for columns that don't have "hyperfeed" in them
def takeline(line):
    idnum = "hyperfeed_story_id_"
    id = line[0]
    if idnum in line[0]:
            id = line[0].strip(idnum)
    else:
            id = id
    return id, line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]

takeline = rdd.map(takeline)
takeline.take(3)


#remove html tags and other weird emojis in "message" content
def remove_paragraph_tags(line):
    fixupcontent = line[4]
    fixupcontent = str(fixupcontent)
    clean = re.compile(r'<.*?>')
    fixupcontent1 = re.sub(clean, '', str(fixupcontent))
    emoji_pattern = re.compile("["
    u"\U0001F600-\U0001F64F" # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "]+", flags=re.UNICODE)
    fixupcontent2 = (emoji_pattern.sub(r'', str(fixupcontent1)))
    return line[0], line[1], line[2], line[3], str(fixupcontent1), line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]


remove_tags = takeline.map(remove_paragraph_tags)
remove_tags.take(1)


remove_tags_df = remove_tags.toDF()
remove_tags_df = remove_tags_df.replace('None',None).na.fill('na')
remove_tags = remove_tags_df.rdd.map(tuple)

def fix_time(line):
    created_time = line[6]
    html = "<"
    url = "pp-facebook-ads"
    en = "en-US" 
    weirdnum = '0.74362338'
    weirdnum1 = 'we become'
    if created_time == 'na':
        created_time = '2021-01-01'
    if any(c.isalpha() for c in created_time):
        created_time = '2021-01-01'
    if created_time is None:
        created_time = '2021-01-01'
    if "." in created_time[:10]:
        created_time = '2021-01-01'
    if len(created_time) != 29:
        created_time = '2021-01-01'
    if '0.99814938' in created_time:
        created_time = '2021-01-01'
    if url in created_time or en in created_time or weirdnum in created_time or weirdnum1 in created_time or html in created_time:
        created_time = '2021-01-01'
    else:    
        created_time = str(created_time)
    strip_create_time = created_time[:10]
    if "." in created_time:
        created_time = '2021-01-01'
    try:
        datetime_create = datetime.datetime.strptime(strip_create_time, '%Y-%m-%d')
    except:
        datetime_create = datetime.datetime.strptime('2021-01-01', '%Y-%m-%d')
    return line[0], line[1], line[2], line[3], line[4], line[5], datetime_create, line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]

fix1 = remove_tags.map(fix_time)


def fix_time1(line):
    html = "<"
    url = "pp-facebook-ads"
    en = "en-US" 
    weirdnum = '0.74362338'
    weirdnum1 = 'we become'
    update_time = line[7]
    if any(c.isalpha() for c in update_time):
        update_time = '2021-01-01'
    if update_time is None:
            update_time = '2021-01-01'
    if html in update_time or url in update_time or en in update_time or weirdnum in update_time or weirdnum1 in update_time:
            update_time = '2021-01-01'
    else:
            update_time = str(update_time)
    strip_update_time = update_time[:10]
    try:
        datetime_update = datetime.datetime.strptime(strip_update_time, '%Y-%m-%d')
    except:
        datetime_update = datetime.datetime.strptime('2021-01-01', '%Y-%m-%d')
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], datetime_update, line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]


fixtime1 = fix1.map(fix_time1)
fixtime1.take(4)

def fixline(line):
    paid = line[18]
    div = "<div>"
    url = "https://pp"
    if paid is None:
            paid = "NA"
    if div in paid:
            paid = "NA"  
    if url in paid:
            paid = "NA"    
    else:
            paid = paid
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], paid, line[19]

fixline = fixtime1.map(fixline)
fixline.take(10)


#taking the brackets out of the image col
def images_column(line):
    image_url = line[9]
    image_url = str(image_url)
    cleanstart = "{"
    cleanend = "}"
    quote = '"'
    if cleanstart or cleanend or quote in image_url:
      image_url = image_url.replace(cleanstart, '')
      image_url = image_url.replace(cleanend, '')
      image_url = image_url.replace(quote, '')
      return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], str(image_url), line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19]

image_col = fixline.map(images_column)
image_col.take(1)  


#for target column, replace [] rows with "NA"
def target_col(line):
    target = line[13]
    brack = "[]"
    if target == brack:
            key = target
            key = "NA"
    else:
            key = target
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], key, line[14], line[15], line[16], line[17], line[18], line[19]
          

target = image_col.map(target_col)


from nltk.corpus import stopwords
import nltk
#nltk.download('stopwords')
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

messages_clean = target.map(removeStopWords)


imm_wb = ["immigration", "aliens", "alien", "deporting", "deported", "deport", 
           "immigrant", "immigrants", "assimilation",
          "citizenship", "visas", "visa", "daca", "refugee","refugees"]

health_wb = [ 'healthcare', 'obamacare',
             'medicaid', 'va', 'privatization', 
             'health', 'care', 'insurance', 'insurers', 'prescriptions',
             'prescription', 'prescription', 'life-saving', 
              'pharmaceutical', 'pharmaceuticals', 'affordable', 'care', 
             'patient', 
             'uninsured', 'medicare', 'out-of-pocket',
             'drug pricing', 'drug price']

econ_wb = ['globalization', 'finance', 'financial', 'bond', 
           'commodity', 'markets', 'financial', 'contagion', 'financial', 'securities', 'security',
            'stock', 'taxation', 'tax', 'taxes', 'taxes',
           'employment', 'employment', 'unemployment', 'unemployment',
           'wage', 'corporate', 'tax', 'capital', 
           'wages', 'wage', 'union', 'nonunion', 
           'accounting', 'business', 'economics', 'marketing', 'capitalism', 'socialism',
           'transit', 'transit', 'transportation',
           'stimulus', 'federal', 'reserve',
           'jobs', 'tariffs', 'tariff', 'economy']

envior_wb = ['green', 'climate', 'fracking', 'energy', 'drilling', 
             'environmental', 'environment', 'EPA', 'epa', 'environmental', 'pollution', 
             'warming', 'environmental', 'sustainability', 'pollution', 
             'energy', 'overpopulation', 'deforestation', 'environmental', 'degradation',
             'pollution', 'carbon', 'biodiversity', 'genetic', 'modification', 'ozone', 
             'depletion', 'mining', 'depletion', 
             'radioactive', 'radioactive', 'nuclear', 'acid',
             'endangered', 'pollution', 'pollution', 
             'litter', 'littering', 'landfill']

social_wb = ['LGBT', 'adoption', 'lesbian', 'gay', 'homosexual', 'marriage', 'straight', 'prolife',
             'pro', 'choice', 'rape', 'abortion', 'sex', 'education', 'birth', 'marriage', 'marriages',
             'planned', 'parenthood', 'gender', 'identity', 
             'equality', 'statutory', 'violence', 'sex', 'contraception', 
             'abstinence', 'transgende', 'hormone', 'hormones', 'biological', 'athlete', 'athletes',
             'death', 'penalty', 'punishment',
             'combat', 'sexual', 'assault', 'sexually', 'assaulted', 'combat', 'confederate',
             'monuments', 'racism', 'separatism', 'suicide', 'euthanasia', 'terminal',
             'diversity', 'warning', 'niqab', 'hijab']

foreign_wb = ['united', 'nations', 'iran', 'foreign', 'israel',
              'boycott', 'soleimani', 'torture', 'nato', 'israe', 'syrian', 'refugees',
              'foreign', 'yemen', 'drones', 'korean', 'terrorism',
              'afghanistan', 'isis', 'Hong', 'Kong', 'extradition',
              'isis', 'ukraine', 'nsa', 'cuba', 'russian', 'airstrikes', 'syria',
              'india', 'jerusalem']
                
crim_wb = ['police', 'body', 'cameras', 'prisons', 'solitary', 'confinement', 'juveniles',
           'criminal', 'prison', 'sentences', 'trafficking', 
           'penalties', 'prison', 'overcrowding', 'traffickers', 'trafficking', 'criminal']

elec_wb = ['foreign', 'lobbying', 'electoral', 'campaign', 'finance', 'voter', 'fraud',
           'foreigners', 'lobbyists', 
           'candidate', 'transparency', 'criminal', 'politicians', 'politician']

sci_wb = ['vaccinations', 'vaccination', 'vaccinated', 'disease', 'diseases', 'gmo', 'nuclear', 
          'space', 'exploration', 'vaccinated', 'space', 
          'vaccines', 'food', 'crop', 'crops',
          'foods', 'breeding', 'genetic', 
          'biotechnology', 'biotech', 'crop', 'crops', 'dna', 
          'biogenetic', 'gene', 'genes', 'epidemic', 'virus', 'renewable', 'nuclear',
          'geothermal', 'wind', 'nuclear', 'power',
          'plants', 'solar', 'hydroelectricity', 'alternative',
          'nasa', 'aerospace', 'aeronautics']

educ_wb = ['student', 'loan', 'student', 'loans', 'college', 'debt', 'tuition', 
           'preschool', 'charter', 'school', 'charter', 'schools', 'school', 'truancy',
           'students', 'education', 'colleges', 'university', 'universities', 'scholarship', 'scholarships',
           'curriculum', 'educational',  'teacher', 'teachers', 
           'professors', 'school', 'schools', 'kindergarten', 'academic', 'homeschooling', 'homeschool', 
           'academics', 'literacy', 'schooling', 'classroom', 'classrooms', 'postgraduate', 'undergraduate',
           'zoning', 'standardized']

dom_wb = ['gun', 'impeachment', 'impeach', 'impeached', 'impeaching',
          'gerrymandering', 'redraw', 'congressional', 'districts',
          'redrawing', 'neutrality',  
          'policies', 'term', 'nsa',
          'whistleblower',
          'whistleblowers', 'gun', 'guns',
          'affirmative',
          'supreme', 'court', 'social',
          'advertising', 'snowden',
          'impeach', 'drug',
          'weapon', 'weapons', 'redistricting', 'medicinal', 'benefits', 'rehabilitation', 'addiction',
          'prevention', 'nsa', 'metadata',
          'surveillance', 'ammunition', 
          'firearm', 'firearms', 'misinformation', 'surveillance', 'warrant',
          'wiretap', 'supreme', 'retirement', 'income']


def classify_immigrant(line):
    try:
        cnt_imm = 0
        cnt_health = 0
        cnt_econ = 0
        cnt_envior = 0
        cnt_social = 0
        cnt_foreign = 0
        cnt_crim = 0
        cnt_elec = 0
        cnt_sci = 0
        cnt_educ = 0
        cnt_dom = 0
        #cnt_trump = 0
        for word in line[5]:
            if word in imm_wb:
                cnt_imm += 1
            if word in health_wb:
                cnt_health += 1
            if word in econ_wb:
                cnt_econ += 1
            if word in envior_wb:
                cnt_envior += 1
            if word in social_wb:
                cnt_social += 1
            if word in foreign_wb:
                cnt_foreign += 1
            if word in crim_wb:
                cnt_crim += 1
            if word in elec_wb:
                cnt_elec += 1
            if word in sci_wb:
                cnt_sci += 1
            if word in educ_wb:
                cnt_educ += 1
            if word in dom_wb:
                cnt_dom += 1
            #if word in trump_wb:
            #    cnt_trump += 1
        # list of the number of occurrences 
        counts = [cnt_imm, cnt_health, cnt_econ, cnt_envior, cnt_social, cnt_foreign, cnt_crim, cnt_elec, cnt_sci, cnt_educ, cnt_dom]
        # list of the classes
        class_names = ["immigration", "healthcare", "economic", "environment", "social", "foreign", "criminal", "electoral", "science", "education", "domestic"] 
        # get the index location of the max element
        da_max = counts.index(max(counts))
        # testing if any of the counts equal each other, or all are zero
        if counts.count(0) == len(counts):
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other' 
        #if len(set(counts)) == len(counts) and counts.count(0) != len(counts):
        #    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'political' 
        # if not, return the class of the max
        else:
            return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], class_names[da_max] 
    except:
        return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], 'other'

############ feed this into classify_ads.py ###############
data_w_imm = messages_clean.map(classify_immigrant)
############ feed this into classify_ads.py ###############









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
#soc_econ = ads_clean_words.filter(lambda x: x[1] == 'immigration' or x[1] == 'healthcare')



#def shrinker(line):
#    return line[0]
#test1 = soc_econ_encoded.map(shrinker)
#hashingTF = HashingTF()
#tf = hashingTF.transform(documents)
#tf.cache()
#idf = IDF().fit(tf)
#tfidf = idf.transform(tf)




ads_clean_words = ads_clean_words.filter(lambda x: x[1] != 'other')
ad_words_df = ads_clean_words.toDF().selectExpr("_1 as message", "_2 as category")

#["immigration: 1", "healthcare: 2", "economic: 3", "environment: 4", "social: 5", "foreign: 6", "criminal: 7", "electoral: 8", "science: 9", "education:10", "domestic: 11"]

def prepareData(line):
    words = ''
    for x in line[0]:
        words = words + str(x) + ' '
    cat = line[1]
    if cat == "immigration":
        return words, 1
    if cat == "healthcare":
        return words, 2
    if cat == "economic":
        return words, 3
    if cat == "environment":
        return words, 4
    if cat == "social":
        return words, 5
    if cat == "foreign":
        return words, 6
    if cat == "criminal":
        return words, 7
    if cat == "electoral":
        return words, 8
    if cat == "science":
        return words, 9
    if cat == "education":
        return words, 10
    if cat == "domestic":
        return words, 11

    

preped_words = ads_clean_words.map(prepareData)
preped_words.take(1)

preped_words_df = preped_words.toDF().selectExpr("_1 as message", "_2 as category")

from pyspark.sql.types import StringType
changedTypedf = preped_words_df.withColumn("message", preped_words_df["message"].cast(StringType()))

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
tokenizer = Tokenizer(inputCol="message", outputCol="words")
wordsData = tokenizer.transform(changedTypedf)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)


train, test = rescaledData.randomSplit([0.7, 0.3], seed = 2018)

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol = 'features', labelCol = 'category', maxIter=10)
lrModel = lr.fit(train)




