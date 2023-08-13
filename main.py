import findspark
import os
import config
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

findspark.init()

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

conf = (SparkConf()
        .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        .setAppName("pyspark_aws")
        .setMaster("local[*]"))
sc = SparkContext(conf=conf)
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
print("modules imported")

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", config.accessKeyId)
hadoopConf.set("fs.s3a.secret.key", config.secretAccessKey)
hadoopConf.set("fs.s3a.endpoint", "s3-us-east-2.amazonaws.com")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark = SparkSession(sc)

emp_df = spark.read.csv("C:\\Users\\YourUsername\\Documents\\emp.dat", header=True, inferSchema=True)

emp_df.write.format("csv").option("header", "true").save("s3a://pysparkcsvs3/pysparks3/emp_csv/emp.csv", mode="overwrite")

s3_df = spark.read.csv("s3a://pysparkcsvs3/pysparks3/emp_csv/emp.csv/", header=True, inferSchema=True)
s3_df.show(5)
