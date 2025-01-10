import folium
from folium.plugins import HeatMap
import pandas as pd
import os
from pyspark.sql.functions import col


# 定义读取多个 CSV 文件并合并到一个 DataFrame 的函数
def read_and_merge_csv(directory_path):
    # 获取目录下所有的 CSV 文件路径
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    # 读取所有 CSV 文件并将它们合并成一个 DataFrame
    df_list = []
    for file in csv_files:
        file_path = os.path.join(directory_path, file)
        df = pd.read_csv(file_path)
        df_list.append(df)
    # 合并所有 DataFrame
    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

# 合并文件夹下的多个csv
directory_path = "D:\Bag\Code\Pyspark\Chicage_Crime_Cleaned.csv"
crime_data = read_and_merge_csv(directory_path)


# ① 犯罪总热力图
m = folium.Map(location=[crime_data['Latitude'].mean(), crime_data['Longitude'].mean()], zoom_start=6)
# 准备热力图数据
# heat_data = crime_data[['Latitude', 'Longitude']].dropna().values.tolist()
heat_data = crime_data[['Latitude', 'Longitude']].values.tolist()
# 添加热力图到地图
HeatMap(heat_data).add_to(m)
# 保存地图到HTML文件
m.save('heatmap_on_map.html')
print("热力图已生成并保存为 heatmap_on_map.html 文件！")


# ② 家暴热力图
# 使用 Pandas 筛选家暴犯罪数据
# domestic_data_pd = crime_data[crime_data["Domestic"] == True][["Latitude", "Longitude"]].dropna()
domestic_data_pd = crime_data[crime_data["Domestic"] == True][["Latitude", "Longitude"]]
# 创建地图并添加热力图
m = folium.Map(location=[domestic_data_pd['Latitude'].mean(), domestic_data_pd['Longitude'].mean()], zoom_start=10)
heat_data = domestic_data_pd[['Latitude', 'Longitude']].values.tolist()
HeatMap(heat_data).add_to(m)
# 保存地图到HTML文件
m.save('domestic_heatmap_on_map.html')
print("家暴犯罪热力图已生成并保存为 domestic_heatmap_on_map.html 文件！")