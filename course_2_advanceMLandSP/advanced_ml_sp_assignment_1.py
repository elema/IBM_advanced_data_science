
# coding: utf-8

# This is the first assgiment for the Coursera course "Advanced Machine Learning and Signal Processing"
# 
# Just execute all cells one after the other and you are done - just note that in the last one you have to update your email address (the one you've used for coursera) and obtain a submittion token, you get this from the programming assingment directly on coursera.

# In[1]:


credentials_1 = {'password':"""4b5403df0d792637f53845b5b93c089d8fc7f225ba85306f0deeaa4518df1804""",
                 'custom_url':'https://ab28a05d-e0f8-43a2-9930-cfa9e2737b8f-bluemix:4b5403df0d792637f53845b5b93c089d8fc7f225ba85306f0deeaa4518df1804@ab28a05d-e0f8-43a2-9930-cfa9e2737b8f-bluemix.cloudant.com',
                 'username':'ab28a05d-e0f8-43a2-9930-cfa9e2737b8f-bluemix' }


# In[2]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_1['custom_url'].split('@')[1])    .config("cloudant.username", credentials_1['username'])    .config("cloudant.password",credentials_1['password'])    .getOrCreate()


# In[3]:


df=spark.read.load('shake', "org.apache.bahir.cloudant")

df.createOrReplaceTempView("df")
spark.sql("SELECT * from df").show()


# In[4]:


get_ipython().system(u'rm -Rf a2_m1.parquet')


# In[5]:


df = df.repartition(1)
df.write.json('a2_m1.json')


# In[6]:


get_ipython().system(u'rm -f rklib.py')
get_ipython().system(u'wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py')


# In[7]:


import zipfile

def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

zipf = zipfile.ZipFile('a2_m1.json.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('a2_m1.json', zipf)
zipf.close()


# In[8]:


get_ipython().system(u'base64 a2_m1.json.zip > a2_m1.json.zip.base64')


# In[11]:


from rklib import submit
key = "1injH2F0EeiLlRJ3eJKoXA"
part = "wNLDt"
email = "yury@chebiryak.name"
secret = "xycGd27xCQ80xMqU"

with open('a2_m1.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, secret, key, part, [part], data)

