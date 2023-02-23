import pandas as pd
import numpy
from datetime import time, timedelta 
df = pd.read_csv("car_insurance.csv")
df.isnull().values.any()
print(df.dropna())

start = df['CallStart'].astype('datetime64')
end = df['CallEnd'].astype('datetime64')
'''t1 = pd.to_datetime(start)
t2 = pd.to_datetime(end)

print (pd.Timedelta(t2 - t1).seconds / 3600.0)'''

df['Duration']= end - start
for i in df['Duration']:
    df['Duration'] = i.mean()

print(df)