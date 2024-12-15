from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import col, sum
from pyspark.sql import functions as F

# 创建 SparkSession
spark = SparkSession.builder.appName("ChicagoCrimeAnalysis").getOrCreate()

# 1.数据概览
# 1.1.读取数据
hdfs_path = "hdfs://localhost:8020/user/hadoop/Chicage_Crime_Data.csv"
crime_data = spark.read.csv(hdfs_path, header=True, inferSchema=True)

total_rows = crime_data.count()
total_colums = len(crime_data.columns)
print("数据行列数：", (total_rows, total_colums))
print("前五行数据：")
crime_data.show(5)

# 1.2.筛选有意义的字段
columns_to_drop = ['Case Number', 'Ward', 'Community Area', 'FBI Code', 'X Coordinate', 'Y Coordinate']
crime_data = crime_data.drop(*columns_to_drop)
print("筛选有意义的字段后，前五行数据：")
crime_data.show(5)


# 2.数据清洗
# 2.1.缺失值
# 2.1.1.检查每列的缺失值数量
missing_counts = crime_data.select(
    *[sum(col(c).isNull().cast("int")).alias(c) for c in crime_data.columns]
)
# 将缺失值数量转换为 DataFrame
missing_counts_df = missing_counts.collect()[0].asDict()
missing_counts_df = spark.createDataFrame([(k, v) for k, v in missing_counts_df.items()], ["Column", "MissingCount"])
# 添加缺失率列
missing_counts_df = missing_counts_df.withColumn(
    "MissingRate(%)", pyspark.sql.functions.round((col("MissingCount") / total_rows) * 100, 2)
)

print("各列缺失值统计和缺失率：")
missing_counts_df.show(n=missing_counts_df.count(), truncate=False) # 显示所有列

# 2.1.2.处理缺失值
crime_data_cleaned = crime_data.dropna()
missing_counts = crime_data_cleaned.select(
    *[sum(col(c).isNull().cast("int")).alias(c) for c in crime_data_cleaned.columns]
)
print("处理后，各列缺失值统计：")
missing_counts.show()
total_rows_cleaned = crime_data_cleaned.count()
total_colums_cleaned = len(crime_data_cleaned.columns)
print("处理后，数据总行列数：", (total_rows_cleaned, total_colums_cleaned))

# 2.2.重复值
# 2.2.1.检查重复值
duplicates_count = crime_data_cleaned.count() - crime_data_cleaned.dropDuplicates().count()
print(f"重复值数量：{duplicates_count}")

# 2.3.唯一值
# 2.3.1.计算每列的唯一值数量
unique_counts = {col_name: crime_data_cleaned.select(col_name).distinct().count() for col_name in crime_data_cleaned.columns}
print("每列唯一值数量：")
for col_name, count in unique_counts.items():
    print(f"{col_name}: {count}")

# 2.3.2.处理
crime_data_cleaned = crime_data_cleaned.drop('ID')

# 2.3.2.查看分类属性的唯一值
columns = ["Primary Type", "Arrest", "Domestic", "District", "Year"]
for column in columns:
    unique_values = crime_data_cleaned.select(column).distinct().orderBy(column).collect()
    print(f"'{column}'列的唯一值分别是:")
    for row in unique_values:
        print(row[column])
    print("\n")

# 2.3.3.处理属性名
crime_data_cleaned = crime_data_cleaned.withColumn(
    "Primary Type",
    F.when(crime_data_cleaned["Primary Type"] == "NON-CRIMINAL (SUBJECT SPECIFIED)", "NON-CRIMINAL")
    .when(crime_data_cleaned["Primary Type"] == "NON - CRIMINAL", "NON-CRIMINAL")
    .when(crime_data_cleaned["Primary Type"] == "CRIM SEXUAL ASSAULT", "CRIMINAL SEXUAL ASSAULT")
    .otherwise(crime_data_cleaned["Primary Type"])
)

unique_values = crime_data_cleaned.select("Primary Type").distinct().orderBy("Primary Type").collect()
print("Primary Type列的唯一值分别是:")
for row in unique_values:
    print(row["Primary Type"])


# 2.4.检查格式并转换
print('数据模式信息（列名、数据类型、是否可为空）：')
crime_data_cleaned.printSchema()

crime_data_cleaned = crime_data_cleaned.withColumn("District", col("District").cast("string"))
crime_data_cleaned = crime_data_cleaned.withColumn("Beat", col("Beat").cast("string"))
print('查看转化后的数据模式信息：')
print(crime_data_cleaned.schema["District"])
print(crime_data_cleaned.schema["Beat"])

# 上传到HDFS中
output_path = "hdfs://localhost:8020/user/hadoop/Chicage_Crime_Cleaned.csv"
crime_data_cleaned.write.mode("overwrite").csv(output_path, header=True)
print('清洗后的数据已成功保存至HDFS。')