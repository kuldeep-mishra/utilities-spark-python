#!/usr/bin/env python
# coding: utf-8
	
# In[11]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[2]:


spark=SparkSession.builder.appName('test').getOrCreate()


# In[3]:


def dropDup(df, tbl, LOGGER='', type=''):
    clm_list=df.columns
    uninque_cols = []
    duplicate_cols = []
    for i in clm_list:
        if i in uninque_cols:
            duplicate_cols.append(i)
        else:
            uninque_cols.append(i)
    print("All Columns :: {}".format(clm_list))
    print("Unique Columns :: {}".format(uninque_cols))
    print("Duplicate Columns :: {}".format(duplicate_cols))
    if type == "View":
        for clm in duplicate_cols:
            df=df.drop(expr(tbl+'.'+clm))
    elif type == "DataFrame":
        for clm in duplicate_cols:
            df = df.drop(tbl[clm])
    else:
        print('Unknow type : {}'.format(type))
        sys.exit(1)
    return df


# In[4]:


l=[(1, "Rajesh",21,"London", 200),
(2, "Suresh",28,"California", 300),
(3, "Sam",26,"Delhi", 250),
(4, "Rajesh",21,"Gurgaon", 350),
(5, "Manish",29,"Bengaluru", 300)]
df1=spark.createDataFrame(l,schema=['Id','Name','Age','Location','version'])


# In[31]:


l2=[(1, "Rajesh",21,"US", 201),
    (1, "Rajesh",21,"UK", 202),
    (2, "Suresh",28,"Montana", 301),
    (3, "Sam",26,"Noida", 258),
    (3, "Sam",26,"Lucknow", 290),
    (3, "Sam",26,"Indore", 280),
    (4, "Rajesh",21,"Gurgaon", 350),
    (5, "Manish",29,"Bengaluru", 300)]
df2=spark.createDataFrame(l2,schema=['Id','Name','Age','Location','version'])


# In[32]:


df3=df1.join(df2, [df1.version<df2.version, df1.Id==df2.Id])


# In[33]:


df3.show()


# In[34]:


df4=dropDup(df3,df1,"","DataFrame")


# In[35]:


df4.show()


# In[36]:


df4.printSchema()


# In[37]:


df5=df4.withColumn('status',when(col("version")%4==0, 'Y').otherwise('T'))


# In[38]:


df5.show()


# In[41]:


df5.orderBy(["Id", "version"], ascending=[1, 1]).show()


# In[ ]:





# In[ ]:





# In[42]:


oschema=df5.schema


# In[43]:


rdd1=df5.rdd.map(list)


# In[46]:


rdd2=rdd1.map(lambda x: (x[0],x))


# In[47]:


for i in rdd2.take(5): print(i)


# In[48]:


rdd3=rdd2.groupByKey()


# In[49]:


for i in rdd3.take(5): print(i[0],i[1])


# In[67]:


def select_until_resolved(l):
    sl=sorted(l, key=lambda x:x[4], reverse=True)
    print("sl = {}".format(sl))
    nl=[]
    for i in sl:
        print("ele = {}".format(sl))
        nl.append(i)
        if i[5] != 'Y' or i[5] != 'N':
            break
        print("nl = {}".format(nl))
    return nl
    


# In[ ]:





# In[68]:


rdd4=rdd3.map(lambda x: (x[0],select_until_resolved(x[1])))


# In[69]:


for i in rdd4.take(10): print(i)


# In[70]:


rdd5=rdd4.flatMap(lambda x:x[1])


# In[71]:


df6=rdd5.toDF(schema=oschema)


# In[ ]:


df6.show()