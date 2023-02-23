import pandas as pd
def manipulate_data():
    df = pd.read_csv("Salaries.csv")

    output_for_fire_dept = df[df.JobTitle.str.contains('FIRE DEPARTMENT').fillna(False)]
    print(output_for_fire_dept)

    count_row_for_fire_dept = output_for_fire_dept.shape[0]  # Gives number of rows
    print("Total_fire_department: ",count_row_for_fire_dept)

    output_for_police_dept = df[df.JobTitle.str.contains('POLICE DEPARTMENT').fillna(False)]
    print(output_for_police_dept)

    count_row_for_police_dept = output_for_police_dept.shape[0]  # Gives number of rows
    print("Total_police_department: ",count_row_for_police_dept)

    print("Ratio: ",count_row_for_fire_dept/count_row_for_police_dept)

    mean_police_dept = output_for_police_dept["BasePay"].mean()
    print("Police dept mean on basepay: ", mean_police_dept)

    mean_fire_dept = output_for_fire_dept["BasePay"].mean()
    print("Fire dept mean on basepay: ", mean_fire_dept)
    
manipulate_data() 