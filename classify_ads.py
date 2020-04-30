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

messages_clean = remove_tags.map(removeStopWords)

imm_wb = ["immigration", "muslim", "border", "entering the country", "aliens", "deporting", 
"sanctuary", "illegal immigrants", "assimilation", "border security", "citizenship", "visas", "visa", "daca", "refugee",
         "refugees"]

health_wb = [ 'pre existing conditions', 'drug price', 'drug prices', 
             'drug price regulation', 'live saving drug', 'life saving drugs', 
             'healthcare', 'mental health', 'obamacare', 'medicaid', 'single payer healthcare', 
             'marijuna', 'safe haven', 'va privatization', 'health care', 'health']

econ_wb = ['globalization', 'international relations', 'trade', 'finance',
           'financial crisis', 'bond market', 'commodity markets', 'financial contagion',
           'financial market', 'securities', 'security markets', 'share market',
           'stock market', 'taxation', 'tax', 'taxes', 'raise taxes', 'anti poverty',
           'poverty', 'welfare', 'employment', 'employment creation', 'unemployment',
           'unemployment rate', 'hires', 'job creation', 'wage', 'minimum wage',
           'paid sick leave', 'corporate tax', 'capital gains', 'capital gains tax',
           'compensation', 'labor costs', 'wages', 'wage data', 'union', 'nonunion', 'accounting',
           'Business Administration', 'business Economics', 'marketing',  'capitalism', 'socialism', 
           'free market', 'real estate', 'real estate markets', 'mass transit', 'transit', 
           'transportation', 'universal basic income', 'overtime pay', 'economic stimulus', 
           'federal reserve', 'domestic jobs', 'tariffs', 'tariff', 'property taxes', 'economy']

envior_wb = ['climate change', 'fracking', 'alternative energy', 'oil drilling', 'environmental', 
           'environment', 'EPA', 'epa', 'environmental protection agency', 'pollution', 'global warming', 
           'environmental management system', 'sustainability', 'air pollution', 'energy', 
           'overpopulation', 'deforestation', 'waste',  'environmental degradation', 'waste management', 
           'water pollution', 'carbon footprint', 'biodiversity', 'genetic modification', 'ozone layer', 
           'ozone layer depletion', 'mining', 'natural resource depletion', 'natural resources', 
           'natural resource', 'radioactive', 'radioactive waste', 'nuclear energy', 'acid rain', 'endangered', 
           'endangered species', 'light pollution', 'noise pollution', 'urban sprawl', 'litter', 'littering', 'landfill']

social_wb = ['LGBT', 'adoption', 'lesbian', 'gay', 'homosexual', 'same sex marriage', 'straight', 'prolife', 
             'pro choice', 'rape', 'abortion', 'sex education', 'birth control', 'marriage','marriages', 
             'civil unions', 'planned parenthood', 'discrimination', 'gender', 'identity', 'beliefs', 'equality',
             'statutory', 'domestic', 'violence', 'sex', 'spouse', 'contraception', 'birth control', 'abstinence',
             'transgende', 'hormone', 'hormones', 'compete', 'biological', 'athlete', 'athletes', 'death penalty',
             'punishment', 'convict', 'convicted' , 'prison', 'life in prison', 'women combat', 'sexual assault',
             'sexually assaulted', 'combat roles', 'confederate', 'flag', 'historical monuments', 'racism', 'separatism',
             'assisted suicide', 'euthanasia', 'terminal illness', 'diversity', 'workplace', 'safe space', 'safe spaces', 
             'trigger warnings', 'trigger warning', 'niqab', 'hijab', 'church',  'religion', 'religions' ]

foreign_wb = ['mandatory military service','united nations','iran','foreign elections','israel boycott','soleimani',
              'torture','nato','israe','miltary spending','syrian refugees','foreign aid','yemen','drones',
              'north korean military strikes','terrorism','afghanistan','isis ground troops',
              'Hong Kong fugitive extradition','war on isis','ukraine','nsa surveillance','cuba',
              'russian airstrikes in syria', 'india arms','jerusalem','f 35']
                
crim_wb = ['police body cameras' , 'private prisons', 'solitary confinement for juveniles', 'criminal voting rights', 
           'mandatory minimum prison sentences', 'drug trafficking penalties', 'prison overcrowding', 'traffickers', 'trafficking', 'criminal']

elec_wb = ['foreign lobbying', 'electoral college', 'campaign finance', 'voter fraud', 'right of foreigners to vote',
              'lobbyists', 'minimum voting age', 'candidate transparency', 'criminal politicians']

sci_wb = ['vaccinations', 'gmo', 'nuclear energy', 'space exploration', 'engineered foods', 'vaccinated', 'space travel']

educ_wb = ['student loan', 'student loans', 'free college', 'student debt', 'tuition',
           'common core', 'pre k', 'preschool', 'charter school', 'charter schools', 'school truancy', 'school']

dom_wb = ['gun control', 'purchasing a gun', 'impeachment', 'impeachment', 'armed teacher', 'armed teachers', 'gerrymandering', 
          'redraw congressional districts', 'redrawing of congressional districts', 'net neutrality', 'drug policy', 
          'drug policies', 'term limit', 'term limits', 'nsa domestic surveillance', 'citizen phone calls', 
          'citizens phone calls', 'muslim surveillance', 'no fly list', 'whisletblower', 'whistleblowers', 
          'gun violence', 'gun liability', 'gun purchase', 'gun purchasing', 'social media regulation', 
          'social media regulators', 'patriot act', 'affirmative action', 'supreme court reform', 'eminent domain', 
          'seize private property', 'social security', 'flag burning', 'burn flag', 'political ads', 'political advertising',
          'snowden', 'air force one', 'nra']

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


data_w_imm = messages_clean.map(classify_immigrant)
#data_w_imm.filter(lambda x: x[21] == 'immigration').count()

def reducer(line):
    return line[4], line[19], line[21]

class_df = data_w_imm.map(reducer).toDF().selectExpr("_1 as message", "_2 as paid_for_by", "_3 as category")
class_df.createOrReplaceTempView("cat")
query = sqlContext.sql("select category, count(category) cnt from cat group by category order by cnt desc")




cats = ["immigration", "healthcare", "economic", "environment", "social", "foreign", "criminal", "electoral", "science", "education", "domestic"] 
the_others = data_w_imm.filter(lambda x: x[21] not in cats)




