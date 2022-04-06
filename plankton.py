from flask import Flask
from flask import request
from flask_restful import Resource, Api, reqparse
import nltk
import nltk.corpus
import nltk.tokenize.punkt
import nltk.stem.snowball
from datetime import datetime
import time
import collections
from nltk.tokenize import WordPunctTokenizer
import string

import pyodbc
app = Flask(__name__)


# Server Variables
server = 'dariel3idiots.database.windows.net'
db = 'darielihub'
uname = 'dariel3diots'
password = 'dariel3idiots!'
driver = '{ODBC Driver 17 for SQL Server}'
qna = dict()
questions = []
users = []
map_users = dict()
map_xpts = {}
process_time = None

# Get default English stopwords and extend with punctuation
nltk.download('stopwords')
stopwords = nltk.corpus.stopwords.words('english')
stopwords.extend(string.punctuation)
stopwords.append('')

# Create tokenizer and stemmer
tokenizer = WordPunctTokenizer()

@app.route("/")
def hello():
    return "Ping Test at: "+datetime.now().strftime("%H:%M")

# # HTTP requestion class
@app.route("/home")
def initial():
    return "Hi, I'm Plankton."

@app.route("/model/request")
def go():

    start = get_time()

    json_body = request.get_json()
    search_q = json_body['question']
    
    
    database_run()
    
    
    similar_q = {}
    expert = {}
    
    
    
    for i in qna:
    
    # =============================================================================
    #             data fetched from the data based is explicitly converted
    #             into a string using str() to ensure no special or illegal
    #             characters cause internal server errors
    # =============================================================================
   
        if (qna[i][1] == ''):
            ideal_q = ''
            ans = ''
        else:
            ideal_q = str(qna[i][0])
            ans = qna[i][1]
    
        sen_ratio = match_sen(search_q,ideal_q)

        if (sen_ratio> 0.3):
            sen_ratio = sen_ratio*100
            similar_q[sen_ratio] = list()
            similar_q[sen_ratio].append(ans)
    
      
    shift_ratio = 0
    for j in map_users:
    
        xpt_obj = map_users[j]
       
        
        xpt = xpt_obj[0]+' '+xpt_obj[1]
        
     
        skill_list = xpt_obj[-1]
        
        
        xpt_ratio = match_sen(search_q,skill_list)
    
    
        if (xpt_ratio> 0.3 and xpt_ratio != shift_ratio):
            xpt_ratio = xpt_ratio*100
            expert[xpt_ratio] = list()
            expert[xpt_ratio].append(xpt)
            xpt_ratio = xpt_ratio/100
 
        if (xpt_ratio> 0.3 and xpt_ratio == shift_ratio):
            xpt_ratio = xpt_ratio*100
            xpt_ratio = xpt_ratio - 0.2123
            expert[xpt_ratio] = list()
            expert[xpt_ratio].append(xpt)
    
        shift_ratio = xpt_ratio
    
    
 
    sort_q = collections.OrderedDict(sorted(similar_q.items(), reverse=True))
    
    
    answers = ''
    counter = 0
    for ans in sort_q:
    
        ans_list = sort_q[ans]
    
        if counter < len(sort_q)-1:
            answers = answers + ans_list[0] + ','
        else:
            answers = answers + ans_list[0]
    
        counter = counter + 1
    
    
    
    sort_xpt = collections.OrderedDict(sorted(expert.items(), reverse=True))
    
    
    experts = ''
    counter = 0
    for xpt in sort_xpt:
        xpt_list = sort_xpt[xpt]
    
        if counter < len(sort_xpt)-1:
            experts = experts + xpt_list[0] + ','
        else:
            experts = experts + xpt_list[0]
    
        counter = counter + 1
    

    end = get_time()
    process_time = (end - start)/1000

    if len(sort_q) == 0 and len(sort_xpt) == 0:
        return {"output": "no search results."}
    else:
    
        if len(sort_q) == 0:
            return {'answer' : "no answer found for question",
                    'expert' : experts,
                    'processing_time' : str(process_time) +' seconds'}
    
        elif len(sort_xpt) == 0:
             return    {'answer' : answers,
                     'expert' : "no experts found",
                     'processing_time' : str(process_time) +' seconds'}
    
        else:
            return {'answer' : answers,
                'expert' : experts,
                'processing_time' : str(process_time) +' seconds'}
        
        
def database_run():
# GET QUESTIONS
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+
                        ';PORT=1433;DATABASE='+db+';UID='+uname+';PWD='+password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT qaID, question, answer FROM dbo.QandA")
            row = cursor.fetchall()
    
            for i in range(len(row)):
                questions.append(row[i])
    
    
            for i in range(len(questions)):
                qna[i] = list()
                qna[i].append(questions[i][-2])
                qna[i].append(questions[i][-1])
    
    
    # GET USERS
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+
                        ';PORT=1433;DATABASE='+db+';UID='+uname+';PWD='+password) as conn:
        with conn.cursor() as cursor:
            cursor.execute('select * from [dbo].[SkillDescription] JOIN [dbo].[Skills] on '+ 
                           '[dbo].[Skills].skillID = [dbo].[SkillDescription].skillID ' +
                           'JOIN '+
                           '[dbo].[userTable] '+
                           'on '+
                           '[dbo].[Skills].userID = [dbo].[userTable].userID')
            row = cursor.fetchall()
    
            for i in range(len(row)):
                users.append(row[i])
    
    print("DATABASE RUN COMPLETED")
    
    
    for i in range(len(users)):
        map_users[users[i][0]] = list()
        map_users[users[i][0]].append(users[i][8])
        map_users[users[i][0]].append(users[i][9])
        map_users[users[i][0]].append(users[i][1])
        
    
    # CALCULATE TIME
def get_time():
  return round(time.time() * 1000)



# SENTENCE SIMILARITY / FIND EXPERT
def match_sen( a, b, threshold=0.5):

    tokens_a = [token.lower().strip(string.punctuation) for token in tokenizer.tokenize(a) \
                    if token.lower().strip(string.punctuation) not in stopwords]
    tokens_b = [token.lower().strip(string.punctuation) for token in tokenizer.tokenize(b) \
                    if token.lower().strip(string.punctuation) not in stopwords]
    
    # Calculate Jaccard similarity
    ratio = len(set(tokens_a).intersection(tokens_b)) / float(len(set(tokens_a).union(tokens_b)))
    # print(ratio)
    return (ratio)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)