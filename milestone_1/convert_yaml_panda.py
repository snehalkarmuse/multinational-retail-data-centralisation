import pandas as pd

from yaml import safe_load

def convert_yaml():
    with open('yaml_example.yaml', 'r') as f:
     df = pd.json_normalize(safe_load(f))
     
    print(df)

convert_yaml()
''' loading yaml then parsing it. lastly normalising it. In log normalization, the given log data is 
converted into consistent representations and categorizations. 
This is done to record errors and other important details that might not otherwise be obvious.
In normalization, parsers are used to collect all important information from a raw log file, whereas 
is the process of breaking down large quantities of log data to make them easier to understand and collect.'''