import mysql.connector
import pandas as pd
class tableCreator:
    def __init__(self):
        self.mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Aditya@19092000",
        database="walkover_project"
        )
        #primary column
        name = input("Enter table name :")
        primary_c_name = input("Enter primary column name :")
        data_type = input("Enter the entry type :")
        self.c=self.mydb.cursor()
        sql="CREATE TABLE %s(%s %s NOT NULL PRIMARY KEY)"%(name,primary_c_name,data_type)
        self.c.execute(sql)
        self.c.fetchall()
        
        
        #column addition
        while(True):
            choice=input("Add Column y/n\n: ")
            
            if(choice=="y"):
                name = input("Enter table name :")
                new_c=input("Enter the new column name: ")
                n_data_type=input("Enter the data type: ")
                
                sql="ALTER TABLE %s ADD %s %s"%(name,new_c,n_data_type)
                self.c.execute(sql)
                self.c.fetchall()
            else:
                break
            
            
            
        #Adding row
        
        while(True):
            choice=input("Add Row y/n\n: ")
            if(choice=="y"):
                table_name=input("Enter table name: ")
                sql="SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`  WHERE `TABLE_SCHEMA`='walkover_project' AND `TABLE_NAME`='%s' order by ordinal_position"%(table_name)
                self.c.execute(sql)
                table_column=self.c.fetchall()
                print("Enter " ,table_column[0][0],end=":")
                p=input()
                sql= "INSERT INTO %s (%s)VALUES (%s)"%(table_name,table_column[0][0],p)
                self.c.execute(sql)


                self.mydb.commit()
                for i in range(1,len(table_column)):
                    print("Enter " ,table_column[i][0],end=":")
                    a=input()
                   
                    sql="update "+ table_name +" set " + table_column[i][0] +" = %s where " +table_column[0][0] +"=%s"
                    val=(a,p)
                   
                    self.c.execute(sql,val)
                    self.c.fetchall()
                    self.mydb.commit()
                     
            else:
                break
            
            
       #show table
        self.view_table=input("Enter table name you want to see: ")
        sql="SELECT * FROM %s"%(self.view_table)

        self.c.execute(sql)

        Unique_Teams_df=pd.DataFrame (self.c.fetchall())
        Unique_Teams_df.columns= [x[0] for x in self.c.description]
        print(Unique_Teams_df)
        
        
         #sorting table according to user
        column_type=input("Enter column type: ")
        if(column_type=="string"):
            print("Enter your choice :")
            x=input()
            if(x=="is eq"):
                self.is_eq_str()
            elif(x=="is not eq"):
                self.is_not_eq_str()
            elif(x=="starts with"):
                self.starts_with_str()
            elif(x=="ends with"):
                self.ends_with_str()
            elif(x=="contain"):
                self.contains_str()
            elif(x=="doesnot contain"):
                self.doesnot_contains_str()
            elif(x=="Null"):
                self.null_str()
            elif(x=="not Null"):
                self.not_null_str()
        elif(column_type=="integer"):
            print("Enter your choice :")
            x=input()
            if(x=="greater than"):
                self.greater_than()
            elif(x=="less than"):
                self.less_than()
            elif(x=="is eq"):
                self.is_eq_int()
            elif(x=="is not eq"):
                self.is_not_eq_int()
            elif(x=="Null"):
                self.null_int()
            elif(x=="not Null"):
                self.not_null_int()
        elif(column_type=="Email"):
            print("Enter your choice :")
            x=input()
            if(x=="is eq"):
                self.is_eq_eml()
            elif(x=="is not eq"):
                self.is_not_eq_eml()
            elif(x=="starts with"):
                self.starts_with_eml()
            elif(x=="ends with"):
                self.ends_with_eml()
            elif(x=="contain"):
                self.contains_eml()
            elif(x=="doesnot contain"):
                self.doesnot_contains_eml()
            elif(x=="Null"):
                self.null_eml()
            elif(x=="not Null"):
                self.not_null_eml()
        elif(column_type=="Boolean"):
            print("Enter your choice :")
            x=input()
            if(x=="true"):
                self.is_true()
            elif(x=="false"):
                self.is_false()
                    
                
            
    def is_eq_str(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter string: ")
            sql="select * from "+self.view_table+ " where "+clm_choice+" =%s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_not_eq_str(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter string: ")
            sql="select * from "+self.view_table+ " where not "+clm_choice+" =%s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def starts_with_str(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter starting alphabet: ")
            str_choice+="%"
            sql="select * from "+self.view_table+ " where  "+clm_choice+" like %s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def ends_with_str(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter last alphabet: ")
            str1_choice="%"+str_choice
            sql="select * from "+self.view_table+ " where  "+clm_choice+" like %s"
            val=(str1_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def contains_str(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter the containing alphabet: ")
            str1_choice="%"+str_choice +"%"
            sql="select * from "+self.view_table+ " where  "+clm_choice+" like %s"
            val=(str1_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def doesnot_contains_str(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter alphabets ,the word should not contains: ")
            str1_choice="%"+str_choice +"%"
            sql="select * from "+self.view_table+ " where not "+clm_choice+" like %s"
            val=(str1_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
            
    def null_str(self):
            clm_choice=input("Enter the column name = :")
            sql="select * from "+self.view_table+ " where "+clm_choice+" is null"
            self.c.execute(sql)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def not_null_str(self):
            clm_choice=input("Enter the column name = :")
            sql="select * from "+self.view_table+ " where "+clm_choice+" is not null"
            self.c.execute(sql)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def greater_than(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter the reference number : ")
            sql="select * from "+self.view_table+ " where "+clm_choice+" > %s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def less_than(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter the reference number : ")
            sql="select * from "+self.view_table+ " where "+clm_choice+" < %s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_eq_int(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter int: ")
            sql="select * from "+self.view_table+ " where "+clm_choice+" =%s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_not_eq_int(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter Integer: ")
            sql="select * from "+self.view_table+ " where not "+clm_choice+" =%s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def null_int(self):
            clm_choice=input("Enter the column name = :")
            sql="select * from "+self.view_table+ " where "+clm_choice+" is null"
            self.c.execute(sql)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def not_null_int(self):
            clm_choice=input("Enter the column name = :")
            sql="select * from "+self.view_table+ " where "+clm_choice+" is not null"
            self.c.execute(sql)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_eq_eml(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter email: ")
            sql="select * from "+self.view_table+ " where "+clm_choice+" =%s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_not_eq_eml(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter email: ")
            sql="select * from "+self.view_table+ " where not "+clm_choice+" =%s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def starts_with_eml(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter starting alphabet for email: ")
            str_choice+="%"
            sql="select * from "+self.view_table+ " where  "+clm_choice+" like %s"
            val=(str_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def ends_with_eml(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter last alphabet for email: ")
            str1_choice="%"+str_choice
            sql="select * from "+self.view_table+ " where  "+clm_choice+" like %s"
            val=(str1_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def contains_eml(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter the containing alphabet for email: ")
            str1_choice="%"+str_choice +"%"
            sql="select * from "+self.view_table+ " where  "+clm_choice+" like %s"
            val=(str1_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def doesnot_contains_eml(self):
            clm_choice=input("Enter the column name = :")
            str_choice=input("Enter alphabets ,the email should not contains: ")
            str1_choice="%"+str_choice +"%"
            sql="select * from "+self.view_table+ " where not "+clm_choice+" like %s"
            val=(str1_choice,)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
            
    def null_eml(self):
            clm_choice=input("Enter the column name = :")
            sql="select * from "+self.view_table+ " where "+clm_choice+" is null"
            self.c.execute(sql)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def not_null_eml(self):
            clm_choice=input("Enter the column name = :")
            sql="select * from "+self.view_table+ " where "+clm_choice+" is not null"
            self.c.execute(sql)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_true(self):
            clm_choice=input("Enter the column name = :")
            
            sql="select * from "+self.view_table+ " where "+clm_choice+" = %s"
            val=("true",)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
    def is_false(self):
            clm_choice=input("Enter the column name = :")
           
            sql="select * from "+self.view_table+ " where "+clm_choice+" = %s"
            val=("false",)
            self.c.execute(sql,val)
            Unique_Teams_df=pd.DataFrame (self.c.fetchall())
            Unique_Teams_df.columns= [x[0] for x in self.c.description]
            print(Unique_Teams_df)
        
        
        
            
        
        

        
            
           

        
        
        
        
        
        
        
       
        
tableCreator();