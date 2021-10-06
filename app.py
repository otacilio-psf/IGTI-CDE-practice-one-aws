#!/usr/bin/env python
# coding: utf-8

# In[45]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# In[46]:


spark = SparkSession.builder.appName("IGTI-pratice-one").getOrCreate()


# In[47]:


df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .option('encoding', 'ISO-8859-1')
      .load("sample_data.csv")
)

df = df.withColumn("year", lit("2019"))


# In[48]:


df.write.mode("overwrite").format("parquet").partitionBy("year").save("consumer-zone/enem")


# In[49]:


spark.stop()


# In[ ]:




