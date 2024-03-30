from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pandas import DataFrame
from matplotlib import pyplot as plt

spark = SparkSession.builder.master("local[*]").appName("Minimarket").getOrCreate()
dataFrame = spark.read.csv("project2Dataset/products.csv", inferSchema = True, header = True)
dataFrame.createOrReplaceTempView("products")

query_results = spark.sql ("select department_id, count(product_id) products_per_department from products group by department_id")
pandas_dataframe = query_results.toPandas()
pandas_dataframe.to_csv("results/results_query1.csv")

pandas_dataframe.plot.bar ()
plt.xlabel ("department_id")
plt.ylabel ("number_of_products")
plt.savefig ("results/query1_diagram.png")