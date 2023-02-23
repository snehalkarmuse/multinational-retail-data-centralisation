import pandas as pd
import numpy as np

df = pd.read_csv("Salaries.csv")
# for finding surname whose firstname greater then 6 character.
def find_surname(name_split):
    for i in name_split:
        if len(i[0]) > 6:
            print(i[0],i[1])

# avoid duplicate
avoid_duplicate = df.drop_duplicates("JobTitle")
print(avoid_duplicate)

name_split = df["EmployeeName"].str.split()
first_name = name_split.str[0]
last_name = name_split.str[-1]

# count employee whose name is John.
count_name_john = df.loc[first_name.str.contains("John", case=False)]
#print("Employee with name John: ",count_name_john.shape[0])



# for finding surname whose firstname greater then 6 character.
def filter_long_names(name) :
    first_and_last = name.split()
    if len(first_and_last[0]) > 6 :
        return first_and_last
    else:
        return ""
    
print(df["EmployeeName"].apply(filter_long_names))


# adding colume called last_updated . formating date with ISO
new_df = df.assign(LastUpdated = "2023-02-17 16:38:40")
print(new_df)

# creating new column time_ratio. of basepay and overtime pay
def find_val():
    find_ratio = df.loc[:,["OvertimePay", "BasePay"]]
    for i in find_ratio.itertuples(index = False):
        val = (i[0]/i[1]) * 100
    return val
v = find_val()
df2 = df.assign(TimeRatio = v)
print(df2)

# new dataframe in one line who has 100k salary.
new_df[df["BasePay"] > 100000]
print(new_df)
# list all columns from the dataframe
col_list = new_df.columns.values
print(col_list)

# add column pay and add all pay except basepay

temp = new_df.loc[:,["OvertimePay","OtherPay"]]
new_df["Pay"] = temp["OvertimePay"] + temp["OtherPay"]
print(new_df)
new_df.info()
# append row
new_row = {'Id':700, 'EmployeeName':'Joe Black','JobTitle':'SERGENT III', 'BasePay':130457.76, 'OvertimePay':4534.00, 'OtherPay':5646.00,
'Benefits':np.nan, 'TotalPay':140637.76,'TotalPayBenefits':np.nan, 'Year':1994, 'Notes':'', 'Agency':'', 'Status':'',
'LastUpdated':'2023-02-17 16:38:40','Pay':10180}

new_df.append(new_row, ignore_index=True)

print(new_df)


