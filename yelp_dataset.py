# Creating sql-like dataframe from csv file
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Lab6').getOrCreate()
df_b = spark.read.option("delimiter", "::").csv('/FileStore/tables/business.csv').toDF('business_id', 'full_address', 'categories')
df_r = spark.read.option("delimiter", "::").csv('/FileStore/tables/review.csv').toDF('review_id', 'user_id', 'business_id', 'stars')
df_u = spark.read.option("delimiter", "::").csv('/FileStore/tables/user.csv').toDF('user_id', 'name', 'url')

# SQL aliases for dataframe 'tables'
df_b.createOrReplaceTempView("df_b")
df_r.createOrReplaceTempView("df_r")


# Question 1 
# List the business_ID, full address, and categories of the Top 10 businesses using the average ratings
# Creating avg_rating column
df_r2 = spark.sql("select df_r.business_id, count(*) as business_ratings, avg(df_r.stars) as avg_rating from df_r group by df_r.business_id order by business_ratings desc").toDF('business_id', 'business_ratings', 'avg_rating')
df_r2.createOrReplaceTempView("df_r2")

# Joining tables to find businesses with highest avg ratings
df_q1 = spark.sql("select distinct df_r2.business_id, df_b.full_address, df_b.categories, df_r2.avg_rating from df_r2 join df_b on df_r2.business_id = df_b.business_id order by df_r2.avg_rating desc").toDF('business_id', 'full_address', 'categories', 'avg_rating')
df_q1.show(10, truncate=False)


# Question 2
# Find top 10 categories which have the most business count
from pyspark.sql import Row
cat = list(df_q1.select('categories').toPandas()['categories'])

# Parsing through categories to extract values
lst1 = []
lst2 = []

for i in cat:
    c1 = i.replace("List(", '').replace(')','')
    lst1.append(c1)
for i in lst1:
    cat = i.split(", ")
    lst2.append(cat)

flatlist=[element for sublist in lst2 for element in sublist]
df_final = sc.parallelize(flatlist)
row = Row("category")
df_final = df_final.map(row).toDF()

df_final = df_final.groupBy("category").count()
df_final = df_final.orderBy(df_final["count"].desc())
df_final.show(10)