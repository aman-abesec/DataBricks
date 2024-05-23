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
