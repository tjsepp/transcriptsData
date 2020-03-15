import pyodbc
import nltk
nltk.download('stopwords')
nltk.download('punkt')
import re
from settings import *
from nltk.corpus import stopwords

class CreateTranscriptData:
    def __init__(self,db_server,db,db_user,db_pass):
        self.server =db_server
        self.db = db
        self.db_user = db_user
        self.db_pass = db_pass
        self.cnxn = self.db_connect()

    def db_connect(self):
        print ('Connecting to server {}').format(self.server)
        connectionString ='DRIVER={{ODBC Driver 17 for SQL Server}};server={};database={};uid={};pwd={}'\
            .format(self.server,self.db,self.db_user,self.db_pass)
        con = pyodbc.connect(connectionString)
        print ('Connected to database {} on {}').format(self.db, self.server)
        cur = con.cursor()
        return cur

    def get_word_frequencies(self,transcript):
        default_stopwords = set(nltk.corpus.stopwords.words('english'))
        fp = self.process_transcript_record(transcript)
        fp = re.sub(r'\d+',' ', fp)
        words = nltk.word_tokenize(fp)
        words = [word for word in words if len(word) > 1]
        # Lowercase all words (default_stopwords are lowercase too)
        words = [word.lower() for word in words]
        # Remove stopwords
        words = [word for word in words if word not in default_stopwords]
        # Calculate frequency distribution
        fdist = nltk.FreqDist(words)
        #print(fdist)
        #print fdist.viewitems()
        #print type(fdist)
        for word, frequency in fdist.most_common(200):
            print(u'{}: {}:{}'.format(transcript,word, frequency))


    def process_transcript_record(self,trans_id):
        query = '''
        SELECT (STUFF((
                SELECT ' ' + componentText
                FROM ciqTranscriptComponent
                WHERE transcriptId = {}
                FOR XML PATH('')
                ), 1, 2, '')
            ) AS StringValue
        '''.format(trans_id)
        #print('Executing Query {}').format(query)
        data = self.cnxn.execute(query)
        row = data.fetchone()
        return row[0].replace('\\','').replace('-','').replace('&amp','&').replace('&#x0D','').replace("'","")

    def insert_into_database(self,dic):
        pass




x = CreateTranscriptData(db_server=DATABASE_SERVER,db=DATABASE_NAME,db_user=DATABASE_USER,db_pass=DATABASE_PASSWORD)
x.get_word_frequencies('1911068')
