
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# Sampling is one of the most important things when it comes to visualization because often the data set get so huge that you simply
# 
# - can't copy all data to a local Spark driver (Watson Studio is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[151]:


def getSample(df,spark):
    df.createOrReplaceTempView("washing")
    res = spark.sql('select ts, temperature from washing where ts is not null order by ts asc').fillna(0)
    #return df.rdd.map(lambda row: (row.ts, row.temperature)).fillna(0).orderBy('ts').sample(False, 0.1)
    return spark.createDataFrame(res.rdd.sample(False, 0.1).map(lambda row: (row.ts, row.temperature)))


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and retur a python list containing all temperature values from the data set

# In[152]:


def getListForHistogramAndBoxPlot(df,spark):
    #df.select('temperature')
    return df.select('temperature').filter('temperature is not null').rdd.map(lambda row: row[0]).collect()


# Finally we want to create a run chart. Please return two lists (encapusalted in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refere to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[153]:


#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart(df,spark):
    #s = getSample(df, spark)
    #rows = s.fillna(0).orderBy('ts').collect()
    #timestamps = [x['ts'] for x in rows]
    #temperatures = [x['temperature'] for x in rows]
    #res_rdd = s.fillna(0).orderBy('ts')
    #timestamps = s.map(lambda (ts, temperature): ts).collect()
    #temperatures = s.map(lambda (ts, temperature): temperature).collect()
    df.createOrReplaceTempView("washing")
    res = spark.sql('select ts, temperature from washing where ts is not null order by ts asc').fillna(0)
    s = res.rdd.sample(False, 0.1).map(lambda row: (row.ts, row.temperature))
    timestamps = s.map(lambda x: x[0]).collect()
    temperatures = s.map(lambda x: x[1]).collect()
    return (timestamps, temperatures)
    


# In[139]:


#s = getSample(df, spark)


# In[140]:


#s.take(10)


# In[122]:


#tss = s.map(lambda x: x[1]).collect()


# In[123]:


#tss


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the object store and read a PARQUET file and create a dataframe out of it. We've created that data for you already. Using SparkSQL you can handle it like a database.

# In[124]:


import ibmos2spark

# @hidden_cell
credentials = {
    'endpoint': 'https://s3-api.us-geo.objectstorage.service.networklayer.com',
    'api_key': 'PUJMZf9PLqN4y-6NUtVlEuq6zFoWhfuecFVMYLBrkxrT',
    'service_id': 'iam-ServiceId-9cd8e66e-3bb4-495a-807a-588692cca4d0',
    'iam_service_endpoint': 'https://iam.bluemix.net/oidc/token'}

configuration_name = 'os_b0f1407510994fd1b793b85137baafb8_configs'
cos = ibmos2spark.CloudObjectStorage(sc, credentials, configuration_name, 'bluemix_cos')

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Since JSON data can be semi-structured and contain additional metadata, it is possible that you might face issues with the DataFrame layout.
# Please read the documentation of 'SparkSession.read()' to learn more about the possibilities to adjust the data loading.
# PySpark documentation: http://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json

df = spark.read.parquet(cos.url('washing.parquet', 'courseradsnew-donotdelete-pr-1hffrnl2pprwut'))
df.show()


# In[141]:


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[142]:


#df.select('temperature').filter('temperature is not null').rdd.map(lambda row: row[0]).collect()


# In[143]:


#s = getSample(df, spark)


# In[144]:


#s


# In[145]:


#df.filter('temperature is not null').filter('ts is not null').o
#timestamps = []
#temperatures = []
#rows = df.fillna(0).orderBy('ts').collect()
#timestamps = [x['ts'] for x in rows]
#temperatures = [x['temperature'] for x in rows]


# In[146]:


#timestamps = [x['ts'] for x in rows]
#temperatures = [x['temperature'] for x in rows]
#timestamps


# In[147]:


#temperatures = [x['temperature'] for x in rows]
#temperatures


# In[154]:


plt.hist(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[155]:


plt.boxplot(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[156]:


lists = getListsForRunChart(df,spark)


# In[157]:


plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and sumbit it to the grader.
