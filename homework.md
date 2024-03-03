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

# Question 3

```python
# import packages
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime

# create spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# import one parquet file among the 6 previous partitions with pandas
pandas_data_frame = pd.read_parquet("/home/arbenkqiku/spark/repartitions/part-00000-9976065f-7c45-483a-a431-773d62b32f06-c000.snappy.parquet")

# apply correct schema to pandas columns
pandas_data_frame['dispatching_base_num'] = pandas_data_frame['dispatching_base_num'].astype(str)
pandas_data_frame['pickup_datetime'] = pd.to_datetime(pandas_data_frame['pickup_datetime'])
pandas_data_frame['dropOff_datetime'] = pd.to_datetime(pandas_data_frame['dropOff_datetime'])
pandas_data_frame['PUlocationID'] = pd.to_numeric(pandas_data_frame['PUlocationID'])
pandas_data_frame['DOlocationID'] = pd.to_numeric(pandas_data_frame['DOlocationID'])
pandas_data_frame['SR_Flag'] = pandas_data_frame['SR_Flag'].astype(str)
pandas_data_frame['Affiliated_base_number'] = pandas_data_frame['Affiliated_base_number'].astype(str)

# prepare schema for spark data frame
schema = types.StructType([
        types.StructField('dispatching_base_num', types.StringType(), True), 
        types.StructField('pickup_datetime', types.TimestampType(), True), 
        types.StructField('dropOff_datetime', types.TimestampType(), True), 
        types.StructField('PULocationID', types.IntegerType(), True), 
        types.StructField('DOLocationID', types.IntegerType(), True), 
        types.StructField('SR_Flag', types.StringType(), True),
        types.StructField('Affiliated_base_number', types.StringType(), True), 
 ])

 # read spark dataframe with correct schem
 df = spark.read. \
    option("header", "true"). \
    option("compression", "gzip"). \
    schema(schema). \
    csv("/home/arbenkqiku/spark_batch/fhv_tripdata_2019-10.csv.gz")

# create temporary table that we can then query with SQL
df.createOrReplaceTempView('table')

# calculate trips per date
spark.sql("""
    select
        date_trunc('day', pickup_datetime) as date,
        count(1) as trips
    from
        table
    group by
        date
    order by
        date
    """
).show()
```

#### Output
```+-------------------+-----+
|               date|trips|
+-------------------+-----+
|2019-10-01 00:00:00|59873|
|2019-10-02 00:00:00|68746|
|2019-10-03 00:00:00|71638|
|2019-10-04 00:00:00|68227|
|2019-10-05 00:00:00|52398|
|2019-10-06 00:00:00|45665|
|2019-10-07 00:00:00|66137|
|2019-10-08 00:00:00|64049|
|2019-10-09 00:00:00|60468|
|2019-10-10 00:00:00|68559|
|2019-10-11 00:00:00|67715|
|2019-10-12 00:00:00|51434|
|2019-10-13 00:00:00|45900|
|2019-10-14 00:00:00|52665|
|2019-10-15 00:00:00|62610|
|2019-10-16 00:00:00|68156|
|2019-10-17 00:00:00|67656|
|2019-10-18 00:00:00|68471|
|2019-10-19 00:00:00|52530|
|2019-10-20 00:00:00|48304|
+-------------------+-----+

```

# Question 4

#### Continue from previous code
```python
# calculate difference in our between and dropOff_datetime and order it in descending order
spark.sql("""
    select
        pickup_datetime,
        dropOff_datetime,
        datediff(hour, pickup_datetime, dropOff_datetime) as date_diff
    from
        table
    order by
        date_diff desc
    """
).show()
```

#### Output
```
+-------------------+-------------------+---------+
|    pickup_datetime|   dropOff_datetime|date_diff|
+-------------------+-------------------+---------+
|2019-10-11 18:00:00|2091-10-11 18:30:00|   631152|
|2019-10-28 09:00:00|2091-10-28 09:30:00|   631152|
|2019-10-31 23:46:33|2029-11-01 00:13:00|    87672|
|2019-10-01 21:43:42|2027-10-01 21:45:23|    70128|
|2019-10-17 14:00:00|2020-10-18 00:00:00|     8794|
|2019-10-26 21:26:00|2020-10-26 21:36:00|     8784|
|2019-10-30 12:30:04|2019-12-30 13:02:08|     1464|
|2019-10-25 07:04:57|2019-12-08 07:21:11|     1056|
|2019-10-25 07:04:57|2019-12-08 07:54:33|     1056|
|2019-10-01 07:21:12|2019-11-03 08:44:21|      793|
|2019-10-01 13:41:00|2019-11-03 14:58:51|      793|
|2019-10-01 13:47:17|2019-11-03 15:20:28|      793|
|2019-10-01 05:18:52|2019-11-03 05:48:17|      792|
|2019-10-01 06:54:57|2019-11-03 07:22:01|      792|
|2019-10-01 05:34:32|2019-11-03 05:49:22|      792|
|2019-10-01 02:30:01|2019-11-03 03:02:02|      792|
|2019-10-01 05:06:06|2019-11-03 05:24:37|      792|
|2019-10-01 04:29:49|2019-11-03 04:56:10|      792|
|2019-10-01 05:36:30|2019-11-03 06:23:36|      792|
|2019-10-01 05:11:04|2019-11-03 05:13:25|      792|
+-------------------+-------------------+---------+
```

# Question 6
#### Download taxi lookup table
```bash
 wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

#### Continue with code from Question 4
```python
# import taxi zone look up
taxi_lookup_df = spark.read \
    .option("header", "true") \
    .csv("taxi_zone_lookup.csv")

# create taxi zone lookup temporary table that we can query with spark sql
taxi_lookup_df.createOrReplaceTempView('look_up_zones')

# join PULocationID with look_up_zones table, count number of trips per PULocationID and order in ascending orer
spark.sql("""
    select
        look_up_zones.Zone as zone,
        count(1) as trips
    from
        table
    left join
        look_up_zones on table.PULocationID = look_up_zones.LocationID
    group by
        zone
    order by
        trips
    
""").show()
```

#### Output
```
+--------------------+-----+
|                zone|trips|
+--------------------+-----+
|         Jamaica Bay|    1|
|Governor's Island...|    2|
| Green-Wood Cemetery|    5|
|       Broad Channel|    8|
|     Highbridge Park|   14|
|        Battery Park|   15|
|Saint Michaels Ce...|   23|
|Breezy Point/Fort...|   25|
|Marine Park/Floyd...|   26|
|        Astoria Park|   29|
|    Inwood Hill Park|   39|
|       Willets Point|   47|
|Forest Park/Highl...|   53|
|  Brooklyn Navy Yard|   57|
|        Crotona Park|   62|
|        Country Club|   77|
|     Freshkills Park|   89|
|       Prospect Park|   98|
|     Columbia Street|  105|
|  South Williamsburg|  110|
+--------------------+-----+
```