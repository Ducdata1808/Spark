from pyspark.sql import SparkSession
import logging
import time
import sys

def vn_time(*args):
    return time.localtime(time.time() + 7 * 3600)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='error.log',
    filemode='a'
)
logging.Formatter.converter = vn_time
logger = logging.getLogger(__name__)

try:  
    spark = SparkSession.builder \
        .appName("read_data") \
        .master("local[*]") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.cores", "1") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
    logger.info("Completely initialize SparkSesion")
except Exception as e:
    logger.error(f"An error occurred: {e}")
    sys.exit(1)

sc = spark.sparkContext
data = ["A", "B", "C", "C", "A"]
logger.info(f"source data was craeted: {data}")

# craete rdd from memory data
try:
    rdd = sc.parallelize(data)
    logger.info(f"rdd was craeted from data with {rdd.getNumPartitions()} partitions")
    rdd.collect()
except Exception as e:
    logger.error(f"An error occurred: {e}")
    sys.exit(1)
# create rdd from HDFS
try:
    rdd_disk = sc.textFile("/tmp/data.txt")
    logger.info(f"rdd from local disk was created with {rdd_disk.getNumPartitions()} partitions")
    rdd_disk.collect()
except Exception as e:
    logger.error(f"An error occurred: {e}")
    sys.exit(1)

# create dataframe from memory
data_df = [
(1, "Le Van Duc", "Ho Chi Minh city", "Vietnam", "18/08/2006", True),
(2, "Nguyen Van A", "Da nang city", "Vietnam", "23/06/2007", False),
(3, "Nguyen Thi B", "Ha Noi capital", "Vietnam", "08/09/2003", True)
]
schema = ["id", "Name", "Current residence", "Nationality", "Birthday", "active status"]
try:
    df = spark.createDataFrame(data_df, schema)
    logger.info("create dataframe from memory successfully")
    df.show()
except Exception as e:
    logger.error(f"an error occurs when create dataframe from memory: {e}")
    sys.exit(1)

# create dataframe from HDFS
try:
          df_hdfs = spark.read\
.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/tmp/advertisement.csv")
          logger.info("crate dataframe from HDFS successfully")
          df_hdfs.show()
except Exception as e:
          logger.error(f"an error occurs when crate dataframe from HDFS: {e}")
          sys.exit(1)
# create datafrmae from GCS bucket
try:
          df_gcs = df = spark.read\
.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("gs://my_spark_data/advertisement.csv")
          logger.info("create dataframe from GCS sucessfully")
          df_gcs.show()
except Exception as e:
          logger.error(f"an error occurs when create dataframe from GSC: {e}")
          sys.exit(1)

# create temporary table
try:
    temp_table_name = "table1"
    df_hdfs.createOrReplaceTempView(temp_table_name)
    result = spark.sql(f"SELECT * FROM {temp_table_name}")
    logger.info("create table successfully")
except Exception as e:
    logger.error(f"an error occurs when create table: {e}")
    sys.exit(1)
    

sc.stop()
