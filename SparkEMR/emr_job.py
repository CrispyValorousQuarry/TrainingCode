from pyspark.sql import SparkSession

# Key change: we do not use .master('local[*]')
# We can either omit the .master() call OR we can use 
# .master('yarn') - both will end up doing the same thing

spark = SparkSession.builder.appName("EMR_Demo").getOrCreate()

#I'm going to write my input and output paths here
#Note: these are paths to my S3 bucket. This code is going
# to run in my Cluster, within AWS
#These are not local file paths
input_path="s3://emr-demo-zk/raw/Warehouse_and_Retail_Sales.csv" # pointing to the csv on my S3 bucket
output_path="s3://emr-demo-zk/processed/warehouse_sales_report" # pointing to my processed directory on my S3 bucket

#Debugging message for later
print(f'Reading data from {input_path}...')

#Reading from our S3 bucket csv, into dataframe. Remember this is running on AWS
sales_data_df=spark.read.csv(input_path,header=True,inferSchema=True)

#For now we end with .show()
sales_data_df.show()
