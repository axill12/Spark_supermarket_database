from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pandas import DataFrame
from matplotlib import pyplot as plt

spark = SparkSession.builder.master("local[*]").appName("Minimarket").getOrCreate()
dataFrame_departments = spark.read.csv("project2Dataset/departments.csv", inferSchema = True, header = True)
dataFrame_departments.createOrReplaceTempView("departments")

dataFrame_products = spark.read.csv("project2Dataset/products.csv", inferSchema = True, header = True)
dataFrame_products.createOrReplaceTempView("products")

dataFrame_order_products = spark.read.csv("project2Dataset/order_products.csv", inferSchema = True, header = True)
dataFrame_order_products.createOrReplaceTempView("order_products")

query_results = spark.sql ("select distinct departments.department_id, products.product_id, product_name from departments, products, order_products where reordered = 0 and departments.department_id = products.department_id and order_products.product_id = products.product_id order by departments.department_id, product_name")
pandas_dataframe = query_results.toPandas()
pandas_dataframe.to_csv("results/results_query3.csv")
