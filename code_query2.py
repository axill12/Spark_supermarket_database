from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pandas import DataFrame
from matplotlib import pyplot as plt

spark = SparkSession.builder.master("local[*]").appName("Minimarket").getOrCreate()
dataFrame = spark.read.csv("project2Dataset/orders.csv", inferSchema = True, header = True)
dataFrame.createOrReplaceTempView("orders")

query_results = spark.sql ("select order_dow, count(order_id) as number_of_orders from orders group by order_dow order by order_dow")
pandas_dataframe = query_results.toPandas()
pandas_dataframe.to_csv("results/results_query2.csv")

pandas_dataframe.plot.bar ()
plt.xlabel ("order_dow")
plt.ylabel ("number_of_orders")
plt.savefig ("results/query2_diagram.png")