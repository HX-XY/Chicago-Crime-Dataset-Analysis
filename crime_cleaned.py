from pyspark.sql import SparkSession
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, count, when, month, year, to_date, udf, hour, to_timestamp


# 创建 SparkSession
spark = SparkSession.builder.appName("Crime").getOrCreate()
# 读取数据（数据基本信息）
hdfs_path = "hdfs://localhost:8020/user/hadoop/Chicage_Crime_Cleaned.csv"
crime_data = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# # 3.数据探索
# # 3.1.犯罪类型 + 抓捕率
# def arrest_rate_groupbycount(group):
#     result = crime_data.groupBy(group).agg(
#         count("*").alias("Count"),
#         (count(when(col("Arrest") == True, 1)) / count("*")).alias("ArrestRate")
#     )
#     # 按类型数量降序排序
#     result = result.orderBy(col("Count").desc())
#     print('类型统计数量及抓捕率：')
#     result.show(n=result.count(), truncate=False) # 显示所有列
#     return result
#
# result_type = arrest_rate_groupbycount("Primary Type")
#
# # 转换为 Pandas DataFrame
# result_type_pd = result_type.toPandas()
# # 提取类型名称、数量和逮捕率
# types = result_type_pd["Primary Type"]
# type_counts = result_type_pd["Count"]
# arrest_rates = result_type_pd["ArrestRate"]
#
# # 创建图形和轴
# fig, ax1 = plt.subplots(figsize=(12, 6))
# # 画类型数量的直方图
# ax1.bar(types, type_counts, color='#c56978', label='Type Count')
# ax1.set_xlabel('Primary Type', fontsize=12)
# ax1.set_ylabel('Type Count', color='#c56978', fontsize=12)
# ax1.tick_params(axis='x', rotation=90)
# # 创建第二个 y 轴用于画逮捕率折线图
# ax2 = ax1.twinx()
# ax2.plot(types, arrest_rates, color='#363532', marker='*', label='Arrest Rate')
# ax2.set_ylabel('Arrest Rate (%)', color='#363532', fontsize=12)
# ax2.tick_params(axis='y', colors='#363532')
# # 添加图例
# ax1.legend(loc='upper left')
# ax2.legend(loc='upper right')
# # 添加标题
# plt.title('Type Count and Arrest Rate by Primary Type', fontsize=14)
# # 显示图形
# plt.tight_layout()
# plt.show()
#
#
# # 3.2.空间分析
# # 3.2.1.热力地图
# # 3.2.2.不同警区发生的犯罪数量 & 抓捕率
# def arrest_rate_groupbyid(group):
#     result = crime_data.groupBy(group).agg(
#         count("*").alias("Count"),
#         (count(when(col("Arrest") == True, 1)) / count("*")).alias("ArrestRate")
#     )
#     # 按id降序排序
#     result = result.orderBy(col(group).desc())
#     print('类型统计数量及抓捕率：')
#     result.show(n=result.count(), truncate=False) # 显示所有列
#     return result
#
# result_district = arrest_rate_groupbyid("District")
# # 转换为 Pandas DataFrame
# result_district_pd = result_district.toPandas()
# # 提取类型名称、数量和逮捕率
# districts = result_district_pd["District"]
# districts_counts = result_district_pd["Count"]
# arrest_rates = result_district_pd["ArrestRate"]
# # 创建图形和轴
# fig, ax1 = plt.subplots(figsize=(12, 6))
# # 画类型数量的直方图
# ax1.bar(districts, districts_counts, color='#c56978', label='Type Count')
# ax1.set_xlabel('District', fontsize=12)
# ax1.set_ylabel('Count', color='#c56978', fontsize=12)
# ax1.tick_params(axis='x')
# # 创建第二个 y 轴用于画逮捕率折线图
# ax2 = ax1.twinx()
# ax2.plot(districts, arrest_rates, color='#363532', marker='*', label='Arrest Rate')
# ax2.set_ylabel('Arrest Rate (%)', color='#363532', fontsize=12)
# ax2.tick_params(axis='y', colors='#363532')
# # 添加图例
# ax1.legend(loc='upper left')
# ax2.legend(loc='upper right')
# # 添加标题
# plt.title('Police District Count and Arrest Rate by District', fontsize=14)
# # 显示图形
# plt.tight_layout()
# plt.show()
#
#
# # 3.2.3.犯罪高发地点类型（Location）
# def group_count(group):
#     result = crime_data.groupBy(group).agg(
#         count("*").alias("Count")
#     )
#     # 按id降序排序
#     result = result.orderBy(col('Count').desc())
#     print('类型统计数量：')
#     result.show(n=result.count(), truncate=False) # 显示所有列
#     return result
#
# result_location = group_count("Location Description")
# # 转换为 Pandas DataFrame
# result_location_pd = result_location.toPandas()
#
# # 计算上四分位点
# upper_quartile = result_location_pd["Count"].quantile(0.75)
#
# # 筛选上四分位数据
# filtered_result_location_pd = result_location_pd[result_location_pd["Count"] >= upper_quartile]
# # print('筛选上四分位数据后：')
# # print(filtered_result_location_pd)
#
# # 提取类型名称、数量和逮捕率
# locations = filtered_result_location_pd["Location Description"]
# locations_counts = filtered_result_location_pd["Count"]
#
# # 创建图形和轴
# fig, ax1 = plt.subplots(figsize=(12, 6))
# # 画类型数量的直方图
# ax1.bar(locations, locations_counts, color='#973444', label='Count')
# ax1.set_xlabel('Location Description', fontsize=12)
# ax1.set_ylabel('Count', color='#973444', fontsize=12)
# ax1.tick_params(axis='x', rotation=90)
# ax1.legend(loc='upper left')
# # 添加标题
# plt.title('Count of Crime Location Types', fontsize=14)
# # 显示图形
# plt.tight_layout()
# plt.show()
#
# # 3.3.时间波动趋势
# # 3.3.1.以年为单位
# # 3.3.1.1.分类后总趋势
# crime_data_by_year_type = crime_data.groupBy("Year", "Primary Type") \
#     .agg(count("*").alias("Count")) \
#     .orderBy("Year", "Primary Type")
# print('不同犯罪类型的')
# crime_data_by_year_type.show(n=crime_data_by_year_type.count(), truncate=False)
#
# year_type_count = crime_data_by_year_type.toPandas()
#
# plt.rcParams['font.sans-serif'] = ['SimHei']  # 用于正常显示中文
# plt.rcParams['axes.unicode_minus'] = False   # 用于正常显示负号
#
# # 设置主题和字体
# sns.set_theme(style='white', font_scale=1.2, rc={'axes.facecolor': (0, 0, 0, 0)})
# # 定义调色板
# palette = sns.color_palette("coolwarm", len(year_type_count["Primary Type"].unique()))
# # 实例化 FacetGrid 对象
# g = sns.FacetGrid(
#     year_type_count,
#     row='Primary Type',
#     hue='Primary Type',
#     aspect=15,
#     height=0.75,
#     palette=palette
# )
# # 绘制密度图
# g.map(sns.kdeplot, 'Year', bw_adjust=0.8, clip_on=False, fill=True, alpha=1, linewidth=1.5)
# g.map(sns.kdeplot, 'Year', clip_on=False, color="w", lw=2, bw_adjust=0.8)
# # 添加水平线
# g.map(plt.axhline, y=0, lw=2, clip_on=False)
# # 为每个密度图添加标签
# def label(x, color, label):
#     ax = plt.gca()
#     ax.text(-0.1, 0.2, label, fontweight='bold', color=color,
#             ha='left', va='center', transform=ax.transAxes)
# g.map(label, 'Year')
# # 调整子图间距
# g.fig.subplots_adjust(hspace=-0.3)
# # 删除多余的轴标签和边框
# g.set_titles("")
# g.set(yticks=[], ylabel='', xlabel="Year")
# g.despine(bottom=True, left=True)
# # 设置标题
# g.fig.suptitle('Crime Count Density by Year and Primary Type', fontsize=16, fontweight='bold', ha='right')
# # 显示图形
# plt.show()
#
# # 3.3.1.2.分类后四分趋势
# # 计算四分位点
# def divide_into_quartiles(dataframe, column):
#     # 计算四分位数
#     quantiles = dataframe.approxQuantile(column, [0.25, 0.5, 0.75], 0.01)
#     q1, q2, q3 = quantiles  # 下四分位数、中位数、上四分位数
#     # 添加列标记数据所属的四分位区间
#     dataframe = dataframe.withColumn(
#         "Quartile",
#         when(col(column) <= q1, "Bottom Quartile")  # 下四分
#         .when((col(column) > q1) & (col(column) <= q2), "Second Quartile")  # 中下四分
#         .when((col(column) > q2) & (col(column) <= q3), "Third Quartile")  # 中上四分
#         .otherwise("Top Quartile")  # 上四分
#     )
#     return dataframe, q1, q2, q3
#
# # 对按犯罪类型统计的结果进行四分位划分
# result_with_quartiles, q1, q2, q3 = divide_into_quartiles(result_type, "Count")
# # 查看四分位区间划分
# print(f"Q1 (25%): {q1}, Q2 (50%): {q2}, Q3 (75%): {q3}")
#
# # 筛选四分数据
# bottom_quartile_data = result_with_quartiles.filter(col("Quartile") == "Bottom Quartile")
# # print('筛选的下四分位数：')
# # bottom_quartile_data.show()
# second_quartile_data = result_with_quartiles.filter(col("Quartile") == "Second Quartile")
# # print('筛选的中下四分位数：')
# # second_quartile_data.show()
# third_quartile_data = result_with_quartiles.filter(col("Quartile") == "Third Quartile")
# # print('筛选的中上四分位数：')
# # third_quartile_data.show()
# top_quartile_data = result_with_quartiles.filter(col("Quartile") == "Top Quartile")
# # print('筛选的上四分位数：')
# # top_quartile_data.show()
#
# filter_type_list = [bottom_quartile_data, second_quartile_data, third_quartile_data, top_quartile_data]
# title_list = ['Bottom Quartile', 'Second Quartile', 'Third Quartile', 'Top Quartile']
# for i in range(len(filter_type_list)):
#     # 提取当前分区数据的犯罪类型列表
#     freq_types = filter_type_list[i].select("Primary Type").distinct()
#     # 在原始数据中筛选这些犯罪类型
#     filtered_crime_data = crime_data_by_year_type.join(
#         freq_types,
#         on=crime_data_by_year_type["Primary Type"] == freq_types["Primary Type"],
#         how="inner"
#     ).select(crime_data_by_year_type["Year"], crime_data_by_year_type["Primary Type"], crime_data_by_year_type["Count"])
#     # 显示统计结果
#     print("按年份和犯罪类型统计的犯罪数量：")
#     filtered_crime_data.show(n=filtered_crime_data.count(), truncate=False)
#
#     filtered_crime_data = filtered_crime_data.toPandas()
#
#     # 设置主题和字体
#     sns.set_theme(style='white', font_scale=1.2, rc={'axes.facecolor': (0, 0, 0, 0)})
#     # 定义调色板
#     palette = sns.color_palette("coolwarm", len(filtered_crime_data["Primary Type"].unique()))
#     # 实例化 FacetGrid 对象
#     g = sns.FacetGrid(
#         filtered_crime_data,
#         row='Primary Type',
#         hue='Primary Type',
#         aspect=15,
#         height=0.75,
#         palette=palette
#     )
#     # 绘制密度图
#     g.map(sns.kdeplot, 'Year', bw_adjust=0.8, clip_on=False, fill=True, alpha=1, linewidth=1.5)
#     g.map(sns.kdeplot, 'Year', clip_on=False, color="w", lw=2, bw_adjust=0.8)
#     # 添加水平线
#     g.map(plt.axhline, y=0, lw=2, clip_on=False)
#     # 为每个密度图添加标签
#     def label(x, color, label):
#         ax = plt.gca()
#         ax.text(-0.1, 0.2, label, fontweight='bold', color=color,
#                 ha='left', va='center', transform=ax.transAxes)
#     g.map(label, 'Year')
#     # 调整子图间距
#     g.fig.subplots_adjust(hspace=-0.3)
#     # 删除多余的轴标签和边框
#     g.set_titles("")
#     g.set(yticks=[], ylabel='', xlabel="Year")
#     g.despine(bottom=True, left=True)
#     # 设置标题
#     g.fig.suptitle(f'Crime Count Density by Year and Primary Type({title_list[i]})', fontsize=16, fontweight='bold', ha='center')
#     # 显示图形
#     plt.show()
#
#
# # 3.3.2.以月为单位
# spark.conf.set("spark.sql.session.timeZone", "UTC")
#
# # 自定义解析函数
# def parse_date(date_str):
#     try:
#         return datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p").date()
#     except ValueError:
#         return None
# # 注册 UDF
# parse_date_udf = udf(parse_date, DateType())
# # 应用自定义解析
# crime_data = crime_data.withColumn("Timestamp", parse_date_udf(col("Date")))
#
# # 将 Date 列转换为日期格式并提取 Month
# crime_data = crime_data.withColumn("Timestamp", to_date(col("Timestamp"), "MM/dd/yyyy HH:mm:ss a"))
# crime_data = crime_data.withColumn("Month", month(col("Timestamp")))
# # 按 Primary Type 和 Month 分组统计
# monthly_type_counts = crime_data.groupBy("Primary Type", "Month").agg(
#     count("*").alias("Count")
# ).orderBy("Primary Type", "Month")
# monthly_type_counts = crime_data.groupBy("Primary Type", "Month").count().alias("Count").orderBy("Primary Type", "Month")
# print("按犯罪类型和月份统计的事件数量：")
# monthly_type_counts.show()
#
# monthly_type_counts_pd = monthly_type_counts.toPandas()
# # 准备雷达图数据
# radar_data = {}
# for crime_type in monthly_type_counts_pd["Primary Type"].unique():
#     type_data = monthly_type_counts_pd[monthly_type_counts_pd["Primary Type"] == crime_type]
#     counts = type_data.set_index("Month")["count"].reindex(range(1, 13), fill_value=0).tolist()
#     radar_data[crime_type] = counts
#
# # 设置雷达图的角度
# angles = np.linspace(0, 2 * np.pi, 12, endpoint=False).tolist()
# angles += angles[:1]  # 闭合图形
#
# # 绘制每种犯罪类型的雷达图
# for crime_type, counts in radar_data.items():
#     counts += counts[:1]  # 闭合图形
#     plt.figure(figsize=(8, 8))
#     ax = plt.subplot(111, polar=True)
#     ax.fill(angles, counts, color='#973444', alpha=0.25)
#     ax.plot(angles, counts, color='#973444', linewidth=2)
#     ax.set_yticks([max(counts) // 4 * i for i in range(1, 5)])
#     ax.set_xticks(angles[:-1])
#     ax.set_xticklabels([f"Month {i}" for i in range(1, 13)])
#     plt.title(f"Monthly Trend for Crime Type: {crime_type}", size=14)
#     plt.show()


# 3.4.家庭暴力
# 3.4.1.发生家庭暴力热力图
# 3.4.2.家庭暴力高发地点类型（Location）
# 筛选家庭暴力数据
domestic_data = crime_data.filter(col("Domestic") == True).select("Location Description")
result_domestic_data = domestic_data.groupBy('Location Description').agg(
    count("*").alias("Count")
)
result_domestic_data = result_domestic_data.orderBy(col('Count').desc())
# # 查看
print('类型统计数量：')
result_domestic_data.show(n=result_domestic_data.count(), truncate=False)

# 转换为 Pandas DataFrame
domestic_location_pd = result_domestic_data.toPandas()

# 计算上四分位点
upper_quartile = domestic_location_pd["Count"].quantile(0.75)

# 筛选上四分位数据
filtered_domestic_location_pd = domestic_location_pd[domestic_location_pd["Count"] >= upper_quartile]
# print('筛选上四分位数据后：')
# print(filtered_domestic_location_pd)

# 提取类型名称、数量和逮捕率
locations = filtered_domestic_location_pd["Location Description"]
locations_counts = filtered_domestic_location_pd["Count"]

# 创建图形和轴
fig, ax1 = plt.subplots(figsize=(12, 6))
# 画类型数量的直方图
ax1.bar(locations, locations_counts, color='#973444', label='Count')
ax1.set_xlabel('Location Description', fontsize=12)
ax1.set_ylabel('Count', color='#973444', fontsize=12)
ax1.tick_params(axis='x', rotation=90)
ax1.legend(loc='upper left')
# 添加标题
plt.title('Count of Domestic Location Types', fontsize=14)
# 显示图形
plt.tight_layout()
plt.show()

# # 3.4.3.矩形热力图
# # 将 Date 列转化为 Timestamp 类型并提取小时
# crime_data_with_hour = crime_data.withColumn(
#     "Hour",
#     hour(to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))
# )
#
# # 检查结果
# print('提取所有数据的小时和犯罪类型')
# crime_data_with_hour.select("Date", "Hour", "Primary Type").show(n=5, truncate=False)
# # 提取家庭暴力的小时和犯罪类型
# domestic_data = crime_data_with_hour.filter(col("Domestic") == True).select("Hour", "Primary Type")
# print('提取家庭暴力的小时和犯罪类型')
# domestic_data.show(n=5, truncate=False)
#
# # 将小时划分为时间段
# time_bins = domestic_data.withColumn(
#     "Time Period",
#     when((col("Hour") > 0) & (col("Hour") <= 5), "Wee Hours(0-5)")
#     .when((col("Hour") > 5) & (col("Hour") <= 8), "Morning(6-8)")
#     .when((col("Hour") > 8) & (col("Hour") <= 11), "Forenoon(9-11)")
#     .when((col("Hour") > 11) & (col("Hour") <= 13), "Noon(12-13)")
#     .when((col("Hour") > 13) & (col("Hour") <= 16), "Afternoon(14-16)")
#     .when((col("Hour") > 16) & (col("Hour") <= 19), "Nightfull(17-19)")
#     .otherwise("Evening(20-24)")
# )
#
# # 统计每时间段每种类型的家庭暴力数量
# domestic_count = time_bins.groupBy("Time Period", "Primary Type").count()
# print('统计每时间段每种类型的家庭暴力数量')
# domestic_count.show(n=15, truncate=False)
#
# # 转换为 Pandas DataFrame
# domestic_count_pd = domestic_count.toPandas()
#
# 显式指定时间段顺序
ordered_periods = ["Wee Hours(0-5)", "Morning(6-8)", "Forenoon(9-11)", "Noon(12-13)", "Afternoon(14-16)", "Nightfull(17-19)", "Evening(20-24)"]
# 构造矩阵并保持时间段顺序
matrix_data = domestic_count_pd.pivot(index="Primary Type", columns="Time Period", values="count").fillna(0)
matrix_data = matrix_data[ordered_periods]
# 按行归一化处理
normalized_matrix = matrix_data.apply(lambda row: row / row.sum(), axis=1)
print('归一化后的矩阵数据\n', normalized_matrix)

# 绘制热力图
plt.figure(figsize=(16, 10))
sns.heatmap(normalized_matrix, cmap="Reds", annot=False, fmt="g", cbar_kws={'label': 'Count'})
plt.xlabel("Hour of the Day", fontsize=12)
plt.ylabel("Primary Type", fontsize=12)
plt.title("Domestic Violence Count by Hour and Crime Type", fontsize=14)
plt.tight_layout()
plt.show()