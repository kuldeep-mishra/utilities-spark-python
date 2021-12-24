##Problem statement: corresponding to each linkedin-url pick the record which has greatest job_count, and other records will be deleted.
#final output shall look like: |uuid picked|uuid deleted|

#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()


# In[2]:


import os
print(os.environ['SPARK_HOME'])
print(os.environ['JAVA_HOME'])


# In[15]:


from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, regexp_replace, unix_timestamp, from_unixtime, rank, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType, BooleanType, DateType, LongType
import pyspark.sql.functions as F


# In[4]:


spark = SparkSession.builder.appName('Test_uuid').getOrCreate()


# In[31]:



simpleData = [  Row(111,"Abc ltd.","abc.com","linkedin.com/company/abc",2),
                Row(112,"Abc","abc.in","linkedin.com/company/abc",10),
                Row(121,"Xyz pvt ltd.","xyz.uk","linkedin.com/company/xyz",4),
                Row(122,"xyz","xyz.in","linkedin.com/company/xyz",12),
                Row(131,"ijk","ijk.co.uk","linkedin.com/company/ijk",0),
                Row(141,"cfc","cfc.co.in","linkedin.com/company/cfc",11),
                Row(142,"cfc","cfc.in","linkedin.com/company/cfc",2),
                Row(143,"Cfc co.","cdf.com","linkedin.com/company/cfc",4)  ]

simpleSchema = StructType([
    StructField("uuid",IntegerType(),True),
    StructField("company_name",StringType(),True),
    StructField("company_website",StringType(),True),
    StructField("company_linkedin_page", StringType(), True),
    StructField("job_count", IntegerType(), True),

  ])


# In[32]:


df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),schema=simpleSchema)
df.printSchema()
df.show()


# In[34]:


WindSpec = Window.partitionBy('company_linkedin_page').orderBy(col('job_count').desc())


# In[35]:


df1=df.withColumn('pref', rank().over(WindSpec))


# In[36]:


df1.show()


# In[38]:


select_uuid_df=df1.select('uuid','company_linkedin_page').filter("pref == 1")
select_uuid_df.show()


# In[39]:



deleted_uuid_df=df1.select('uuid','company_linkedin_page').filter("pref > 1")
deleted_uuid_df.show()


# In[48]:


final_df=select_uuid_df.join(deleted_uuid_df, 'company_linkedin_page').drop(deleted_uuid_df.company_linkedin_page).drop(select_uuid_df.company_linkedin_page)
final_df.select(select_uuid_df.uuid, deleted_uuid_df.uuid.alias('deleted_uuid')).show()


# In[49]:


df.createTempView('cmp')


# In[74]:


# spark.sql("select  uuid, rank() over (PARTIONED BY company_linkedin_page ORDER BY job_count desc) as rnk from cmp").show()
# spark.sql("select * from cmp").show()
spark.sql("select * from (SELECT *, rank() over (PARTITION BY company_linkedin_page ORDER BY job_count desc) AS rank FROM cmp) where rank == 1").show()


# In[76]:


fdf=spark.sql("""
Select a.uuid as uuid, b.uuid as deleted_uuid from 
(Select uuid,company_linkedin_page, pref from 
        (Select  *, rank() over (partition by company_linkedin_page order by job_count desc) as pref from cmp) 
    where pref ==  1) a 
Join
(Select uuid,company_linkedin_page, pref from 
        (Select  *, rank() over (partition by company_linkedin_page order by job_count desc) as pref from cmp) 
    where pref >  1) b 
ON a.company_linkedin_page == b.company_linkedin_page


""")
fdf.show()

####output
# +----+------------+
# |uuid|deleted_uuid|
# +----+------------+
# | 112|         111|
# | 122|         121|
# | 141|         143|
# | 141|         142|
# +----+------------+