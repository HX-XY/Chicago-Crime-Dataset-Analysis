# 导入 PySpark 的相关模块
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofweek, hour, when
# 用于机器学习的特征工程和模型
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
# 用于执行 HDFS 命令并解析输出。
import subprocess
import re

# 初始化 SparkSession
spark = SparkSession.builder.appName("ChicagoCrimeAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020") \
    .getOrCreate()

# 设置日志级别为WARN，减少控制台输出
spark.sparkContext.setLogLevel("WARN")

# HDFS 目录路径
hdfs_dir = "hdfs://localhost:8020/user/hadoop/Chicage_Crime_Cleaned.csv"
# 调用 HDFS 命令获取文件列表
cmd = f"hdfs dfs -ls {hdfs_dir}"
process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

# 提取文件路径
partitions = [
    line.split()[-1]
    for line in stdout.decode("utf-8").splitlines()
    if re.search(r"part-\d+", line)
]

# 初始化评估指标列表
auc_list = []
i = 1

# 循环处理每个分区
for partition in partitions:
    print(f"Processing {i}: {partition}...")
    i += 1

    # 加载分区数据
    data = spark.read.csv(partition, header=True, inferSchema=True).limit(10000)
    # 缓存数据以减少重复计算
    data.cache()

    # 转换 Date 列为 Timestamp 类型
    data = data.withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
    # 提取时间特征
    data = data.withColumn("Year", year(col("Date"))) \
               .withColumn("Month", month(col("Date"))) \
               .withColumn("DayOfWeek", dayofweek(col("Date"))) \
               .withColumn("Hour", hour(col("Date")))

    # 将目标变量 Domestic 转换为数值类型
    data = data.withColumn("DomesticNumeric", when(col("Domestic") == True, 1).otherwise(0))

    # 编码字符串特征
    primary_type_indexer = StringIndexer(inputCol="Primary Type", outputCol="PrimaryTypeIndex", handleInvalid="skip")
    location_indexer = StringIndexer(inputCol="Location", outputCol="LocationIndex", handleInvalid="skip")
    # 独热编码
    primary_type_encoder = OneHotEncoder(inputCol="PrimaryTypeIndex", outputCol="PrimaryTypeVec")
    location_encoder = OneHotEncoder(inputCol="LocationIndex", outputCol="LocationVec")
    # 特征整合
    feature_cols = ["PrimaryTypeVec", "LocationVec", "Year", "Month", "DayOfWeek", "Hour"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # 数据集划分
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
    # 定义随机森林分类器
    rf = RandomForestClassifier(featuresCol="features", labelCol="DomesticNumeric", numTrees=50, maxDepth=15, seed=42)
    # 构建 Pipeline
    pipeline = Pipeline(stages=[primary_type_indexer, location_indexer, primary_type_encoder, location_encoder, assembler, rf])
    # 模型训练
    model = pipeline.fit(train_data)
    # 模型评估
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="DomesticNumeric", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)
    print(f"AUC: {auc:.4f}")
    auc_list.append(auc)

    # 释放缓存
    data.unpersist()

# 计算所有分区 AUC 的平均值
average_auc = sum(auc_list) / len(auc_list)
print(f"Average AUC across all partitions: {average_auc:.4f}")