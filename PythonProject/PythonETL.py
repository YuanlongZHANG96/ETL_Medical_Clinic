
# import required module
import os
import pandas as pd
import re
import pyodbc

# assign directory
directory = 'C:\_BISolutions\ETLFinal_YuanlongZhang\DataFiles\ClinicDailyData'

StagingData = pd.DataFrame()
StagingNewPatient = pd.DataFrame()
StagingVisits = pd.DataFrame()


# iterate over files in
# that directory
for root, dirs, files in os.walk(directory):
    for filename in files:
        StagingData = pd.read_csv(root + "\\" + filename)
        StagingData["Date"] = filename[:8]
        StagingData["Clinic"] = root.split("\\")[-1]
        if filename[8:11] == "New":
            frames = [StagingNewPatient, StagingData]
            StagingNewPatient = pd.concat(frames)

        if filename[8:11] == "Vis":
            frames = [StagingVisits, StagingData]
            StagingVisits = pd.concat(frames)

dfnew = pd.DataFrame()
dferror = pd.DataFrame()
i, j = StagingNewPatient.shape
for row in range(0,i):
    temp = StagingNewPatient.iloc[row,:]
    if(re.search('^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$',temp["Email"])):
         frames = [dfnew, temp]
         dfnew = pd.concat(frames,axis=1)
    else:
        def calculate_area(row):
            return "(error)" + row['Email']
        temp["Email"] = temp["Email"] + " (error)"
        frames = [dfnew, temp]
        dfnew = pd.concat(frames, axis=1)

# print(StagingNewPatient)
df_corrected = dfnew.transpose()
df_error = dferror.transpose()

# Load the data to SQL Server
# Connect to SQL
con_str = ("Driver={SQL Server Native Client 11.0};"
           "Server=localhost;"
           "Database=tempdb;"
           "Trusted_Connection=yes;")
con_obj = pyodbc.connect(con_str)

# Create (update or delete) uses a cursor
cusor_obj = con_obj.cursor()
# cusor_obj.execute('''
#         USE [master];
#         If Exists (Select * from Sysdatabases Where Name = 'Staging')
#             Begin
#                 ALTER DATABASE [Staging] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
#                 DROP DATABASE [Staging]
#             End
#         Create Database [Staging];
#         ALTER DATABASE Staging SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
#         ALTER DATABASE Staging collate SQL_Latin1_General_CP1_CI_AS;
#         ALTER DATABASE Staging SET MULTI_USER;
#         ''')
cusor_obj.execute("Use Staging;If (object_id('StagingNewPatient') is not null) Drop Table StagingNewPatient;")
SQL_str = '''Create Table StagingNewPatient
            (FName nvarchar(50)
            ,LName nvarchar(50)
            ,Email nvarchar(100)
            ,Address nvarchar(100)
            ,City nvarchar(50)
            ,State nvarchar(50)
            ,ZipCode nvarchar(20)
            ,Date nvarchar(20)
            ,Clinic nvarchar(20) )
          '''
cusor_obj.execute(SQL_str)

cusor_obj.execute("Use Staging;If (object_id('StagingVisits') is not null) Drop Table StagingVisits;")
SQL_str = '''Create Table StagingVisits
            (Time time
            ,Patient int
            ,Doctor int
            ,[Procedure] int
            ,Charge int
            ,Date int
            ,Clinic nvarchar(20) )
          '''
cusor_obj.execute(SQL_str)

# Insert Dataframe into SQL Server:
SQL_str = ''' INSERT INTO Staging.dbo.StagingNewPatient 
            (FName, LName, Email, Address, City, State, ZipCode, Date, Clinic) 
            values(?,?,?,?,?,?,?,?,?) '''
for index, row in df_corrected.iterrows():
    cusor_obj.execute(SQL_str, row.FName, row.LName, row.Email, row.Address, row.City, row.State,
                      str(row.ZipCode), str(row.Date), row.Clinic)

con_obj.commit()

# Insert Dataframe into SQL Server:
SQL_str = ''' INSERT INTO Staging.dbo.StagingVisits 
            (Time, Patient, Doctor, [Procedure], Charge, Date, Clinic) 
            values(?,?,?,?,?,?,?) '''
for index, row in StagingVisits.iterrows():
    cusor_obj.execute(SQL_str, row.Time, row.Patient, row.Doctor, row.Procedure, row.Charge, row.Date, row.Clinic)

con_obj.commit()
cusor_obj.close()

# Save ErrorReport data to a new file.
# df_error.to_csv('C:\_BISolutions\ETLFinal_YuanlongZhang\ErrorReport\ErrorReport.csv')

# print(df_corrected)
# print(df_error)
# print(StagingVisits)

