import requests
import pandas as pd


#print(response.content)

df = pd.read_csv("test.csv")
print(len(df['crash_record_id'].unique()), len(df))

