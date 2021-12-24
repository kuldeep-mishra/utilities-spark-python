from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lag, col

spark = SparkSession.builder.appName("TEST").getOrCreate()
rdd=spark.sparkContext.textFile("file:///C:\\Users\\product.csv")
header = rdd.take(1)[0]
dates = header.split(',')[2:]

def fun(l):
    return list(zip(dates,l))

mrd = rdd.filter(lambda x : x != header )
mrdd = mrd.map(lambda x : x.split(','))\
    .map(lambda x : ((x[:2]),(x[2:])))

mmrd = mrdd.map(lambda x : (x[0],fun(x[1])))
fmrd = mmrd.flatMapValues(lambda x: x)
allv=fmrd.map(lambda x: (x[0][0],x[0][1],x[1][0],x[1][1]))

df=allv.toDF(schema=['product','model','date','sales'])
df.show(21)

w = Window().partitionBy('model','product').orderBy('product','model','date')
dff=df.select("*", lag('sales').over(w).alias("pds"))
print("dff")
dff.show()
dfs=dff.withColumn('sales_diff', col('sales').cast("Int") - col('pds').cast("Int"))
dfn=dfs.fillna('NA').select('product','model','date','sales',col('sales_diff').alias('increament')).orderBy('product','model','date')

dfn.show(28)