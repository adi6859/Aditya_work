
import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Aditya@19092000",
        database="walkover_project"
        )

c=mydb.cursor()


view_table=input("Enter table name you want to see: ")

sql="SELECT * FROM %s"%(view_table)

c.execute(sql)

Unique_Teams_df=pd.DataFrame (c.fetchall())
Unique_Teams_df.columns= [x[0] for x in c.description]
print(Unique_Teams_df)

def is_eq_str():
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter string: ")
            sql="select * from "+view_table+ " where "+clm_choice+" =%s"
            val=(str_choice,)
            c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in c.description]
            print(Unique_Teams_df)
is_eq_str()