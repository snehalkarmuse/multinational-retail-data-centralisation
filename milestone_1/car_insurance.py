import pandas as pd
import numpy
df = pd.read_csv("car_insurance.csv")
#info() is information about table
print(df.info())
# prints boolean value
df.isnull().values.any()
# isna( checks null value. no column in df so whole table. sum() checking null values in whole table)
na_count = df.isna().sum().sum()
print("Number of null in a table ",na_count)
na_count_age = df['Education'].isna().sum()
print("number of null in Education colmun ", na_count_age)
# dropna() deleters those rows whose value is null
print(df.dropna())
print("Mean of Balance", df['Balance'].mean())
print("Median of Age: ",df['Age'].median())
print("Quartiles last conatct day",df['LastContactDay'].quantile())
df2 = df.groupby(['Job']).sum()
print(df2)
