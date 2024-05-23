# DataBricks

### Creating Data Frame

#### Method-1
``` python
data=[(1,'Aman'),(2,'Akash')]
hedaer_schema = ['id','Name']
df = spark.createDataFrame(data,hedaer_schema)
df.show()
```

#### Method-2
``` python
data=[{'id':1,'Name':'Aman'},{'id':2,'Name':'Akash'}]
df = spark.createDataFrame(data)
df.show()
```

#### Method-3
``` python
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
data=[{'id':1,'Name':'Aman'},{'id':2,'Name':'Akash'}]
hedaer_schema = StructType([StructField(name='id',dataType=IntegerType()),
  StructField(name='Name',dataType=StringType())
])
df = spark.createDataFrame(data)
df.show()
```

### Reading Csv

#### Method-1
```python
path = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
df = spark.read.csv(path,header=True,inferSchema=True)
display(df)
```

#### Method-2
```python
path = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
df = spark.read.format('csv').option(key='header',value=True).load(path)
display(df)
```

### Reading Multiple Csv
```python
#Schema should be same
path1 = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
path2 = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
df = spark.read.csv(path=[path1,path2],header=True,inferSchema=True)
display(df)

#All file schema should be same in this folder
folder_path = 'dbfs:/FileStore/tables/';
df = spark.read.csv(path=folder_path,header=True,inferSchema=True)
display(df)
```
Note : We can also pass schema if we want to read only some columns

### Write dataframe into Csv
```python
#Creating Data frame
data=[(1,'Aman'),(2,'Akash')]
hedaer_schema = ['id','Name']
path='dbfs:/FileStore/tables/maydata'
df = spark.createDataFrame(data,hedaer_schema)
df.write.option("header",True).csv(path,header=True,mode='overWrite')
# For reading data you can just provide the folder path
```

NOTE : Read ,write operation for json 

#### show()
```python
path = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
df=spark.read.csv(path=path,header=True,inferSchema=True);
df.show(n=4,truncate=False,vertical=True) #n number of colums,vertical direction of data to show
```

#### withColumn()
```python
from pyspark.sql.functions import col
path = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
df=spark.read.csv(path=path,header=True,inferSchema=True);
df=df.withColumn('Updated Value',col=col('Value')/100) #Here we can also cast the value .cast('Integer')
df.show(n=2,vertical=True)
```

#### withColumnRenamed()
```python
path = 'dbfs:/FileStore/tables/effects_of_covid_19_on_trade_at_15_december_2021_provisional.csv';
df=spark.read.csv(path=path,header=True,inferSchema=True);
df=df.withColumnRenamed('Country','RenamedCountry')
df.show()
```
