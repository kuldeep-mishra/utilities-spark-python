#pick records belonging to the latest version of file.
#filename : xxxxtYYv[_x] 
		# - 1-4 chars represents dbname(xxxx)
		# - 5th char represent tablename (t)
		# - 6-7 represents year (20YY) 
		# - 8th char represents version (v)
		# - 9-10 chars are option (for future use(ignore for now))
			
#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()
import pyspark


# In[2]:


from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, regexp_replace, unix_timestamp, from_unixtime, rank, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType, BooleanType, DateType, LongType


# In[3]:


spark = SparkSession.builder.appName('Test_uuid').getOrCreate()


# In[9]:



simpleData = [  Row(111,"Abc ltd.","abc.com","linkedin.com/company/abc",2,'abcda200_a'),
                Row(112,"Abc","abc.in","linkedin.com/company/abc",10,'abcda201_a'),
                Row(121,"Xyz pvt ltd.","xyz.uk","linkedin.com/company/xyz",4,'abcda192_a'),
                Row(122,"xyz","xyz.in","linkedin.com/company/xyz",12,'abcda191_a'),
                Row(131,"ijk","ijk.co.uk","linkedin.com/company/ijk",0,'abcda190_a'),
                Row(141,"cfc","cfc.co.in","linkedin.com/company/cfc",11,'abcda181'),
                Row(142,"cfc","cfc.in","linkedin.com/company/cfc",2,'abcda182'),
                Row(143,"Cfc co.","cdf.com","linkedin.com/company/cfc",4,'abcda183')  ]

simpleSchema = StructType([
    StructField("uuid",IntegerType(),True),
    StructField("company_name",StringType(),True),
    StructField("company_website",StringType(),True),
    StructField("company_linkedin_page", StringType(), True),
    StructField("job_count", IntegerType(), True),
    StructField("filename", StringType(), True),

  ])


# In[10]:


df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),schema=simpleSchema)
df.printSchema()
df.show()


# In[11]:


df.select('filename').show()


# In[13]:


df.select('filename').distinct().show()


# In[34]:


files=df.select('filename').distinct().collect()
print(files)
LOF=[f[0] for f in files]
print(LOF)


# In[63]:

def 
d={}
FTK=[] #files to keep
FTR=[] #files to remove
for f in LOF: 
    #print(f)
    #print(f[0])
    #print(f[0][:7],f[0][7])
    
    if f[:7] in d.keys():
        #print("Inside dictionary")
        if d[f[:7]] > f[7]:
            FTR.append(f)
        elif d[f[:7]] < f[7]:
            if d[f[:7]+'etc'] == "NA":
                OF=f[:7]+d[f[:7]]
            else:
                OF=f[:7]+d[f[:7]]+d[f[:7]+'etc']
            FTR.append(OF)
            d[f[:7]] = f[7]
    else:
        #print("Fresh entry")
        d[f[:7]]=f[7]
        if len(f)>8:
            d[f[:7]+'etc']=f[8:]
        else:
            d[f[:7]+'etc']="NA"
    #print(d)
        
    #d[f[0][:7]]= list(d[f[0][:7]]).append()
    #tablename=f[0][:4]
    #year=f[0][4:5]
    #version=f[0][6]

print("LIST OF FILES :: ", LOF)
print("FILES TO REMOVE :: ", FTR)
#print("FILES TO KEEP ::", LOF - FTR)


# In[64]:


df2=df.filter(~df.filename.isin(FTR))
df2.show()


# In[65]:


FTK = [f for f in LOF  if f not in FTR]
print(FTK)


# In[19]:


#dbname=f[0][:4]
#tablename=f[0][:4]
#year=f[0][4:5]
#version=f[0][6]


# In[67]:


df.rdd.getNumPartitions()


# In[ ]:




