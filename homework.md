# Question 1

#### Create a spark shell sessions

```bash
spark-shell
```

#### Get the spark version

```scala
spark.version
```

#### Output
res0: String = 3.4.0

# Question 2

#### Download fhv_2019
```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz
```

#### Create jupyter notebook
```python
# Import packages
import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# Create spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# Import file
df = spark.read.option("header", "true") \
    .option("compression", "gzip") \
    .csv("/home/arbenkqiku/spark_batch/fhv_tripdata_2019-10.csv.gz")

# Repartition it
df = df.repartition(6)

# Export it
df.write.parquet("repartitions/")
```

#### Display the size of each file in the repartitions/ folder
```bash
ls -lh | awk '{print $5, $9}'
```

#### Output
```bash
0 _SUCCESS
6.5M part-00000-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet
6.5M part-00001-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet
6.5M part-00002-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet
6.5M part-00003-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet
6.5M part-00004-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet
6.5M part-00005-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet
```