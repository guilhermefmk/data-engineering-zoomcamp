import pandas as pd


df = pd.read_csv("/home/guilhermefmk/Área de Trabalho/workspace/data-engineering-zoomcamp/week_2/data_flow/yellow/yellow_tripdata_2019-02.csv")
df1 = pd.read_csv("/home/guilhermefmk/Área de Trabalho/workspace/data-engineering-zoomcamp/week_2/data_flow/yellow/yellow_tripdata_2019-03.csv")
print(len(df.index) + len(df1.index))