'''*************************************************************************--
-- Desc: This script connects to MongoDB Atlas cloud and Perform ETL procedures
-- Change Log: When,Who,What
-- 2020-05-15,RRoot,Created File
-- 2022-02-21,Yuanlong Zhang, Updated the File to add new functions for ETL.
-- 2022-03-09,Yuanlong Zhang, Uodated the file for final project.
--**************************************************************************'''

# Please install the package:
# Python -m pip install pyodbc

import pymongo
import pandas as pd
import pyodbc

try:
        # Connect to SQL
        con_str = ("Driver={SQL Server Native Client 11.0};"
                    "Server=localhost;"
                    "Database=DWClinicReportDataYuanlongZhang;"
                    "Trusted_Connection=yes;")
        con_obj = pyodbc.connect(con_str)

        # Create (update or delete) uses a cursor
        cusor_obj = con_obj.cursor()
        cusor_obj.execute("IF (Object_ID('vRptDoctorShifts') is not null) Drop View vRptDoctorShifts;")
        SQL_str = '''Create View vRptDoctorShifts
                        AS
                        Select 
                        [ShiftDate] = Cast(Cast([FullDate] as date) as varchar(100))
                        ,[ClinicName] = dc.ClinicName
                        ,[ClinicCity] = dc.ClinicCity
                        ,[ClinicState] = dc.ClinicState
                        ,[ShiftID] = ds.ShiftID
                        ,[ShiftStart] = ds.ShiftStart
                        ,[ShiftEnd] = ds.ShiftEnd
                        ,[DoctorFullName] = ddo.DoctorFullName
                        ,[HoursWorked] = fds.HoursWorked
                        FROM
                        dbo.FactDoctorShifts fds
                        JOIN dbo.DimDates dd
                        ON fds.ShiftDateKey = dd.DateKey
                        JOIN dbo.DimClinics dc
                        ON fds.ClinicKey = dc.ClinicID
                        JOIN dbo.DimShifts ds
                        ON fds.ShiftKey = ds.ShiftID
                        JOIN dbo.DimDoctors ddo
                        ON fds.DoctorKey = ddo.DoctorKey;'''
        cusor_obj.execute(SQL_str)

        # Select from SQL Server using the Pandas module!
        vRptDoctorShifts = pd.read_sql("select * from vRptDoctorShifts;", con_obj)
        #print(vRptDoctorShifts)

        cusor_obj.execute("IF (Object_ID('vRptPatientVisits') is not null) Drop View vRptPatientVisits;")
        SQL_str = '''Create View vRptPatientVisits
                        AS
                        Select 
                        [VisitDate] = Cast(Cast([FullDate] as date) as varchar(100))
                        ,[ClinicName] = dc.ClinicName
                        ,[ClinicCity] = dc.ClinicCity
                        ,[ClinicState] = dc.ClinicState
                        ,[ProcedureName] = dp.ProcedureName
                        ,[PatientFullName] = dpt.PatientFullName
                        ,[PatientCity] = dpt.PatientCity
                        ,[PatientState] =dpt.PatientState
                        ,[DoctorFullName] = ddo.DoctorFullName
                        ,[ProcedureVisitCharge] = fv.ProcedureVistCharge
                        FROM
                        dbo.FactVisits fv
                        JOIN dbo.DimDates dd
                        ON fv.DateKey = dd.DateKey
                        JOIN dbo.DimClinics dc
                        ON fv.ClinicKey = dc.ClinicID
                        JOIN dbo.DimProcedures dp
                        ON fv.ProcedureKey = dp.ProcedureID
                        JOIN dbo.DimDoctors ddo
                        ON fv.DoctorKey = ddo.DoctorID
                        JOIN dbo.DimPatients dpt
                        ON fv.PatientKey = dpt.PatientID;'''
        cusor_obj.execute(SQL_str)

        # Select from SQL Server using the Pandas module!
        vRptPatientVisits = pd.read_sql("select * from vRptPatientVisits;", con_obj)
        #print(vRptPatientVisits)

        # Always clean up your objects when done!
        cusor_obj.close()
        con_obj.close()

        vRptPatientVisits.to_csv('C:\\_BISolutions\\ETLFinal_YuanlongZhang\\DataFiles\\Staging\\vRptPatientVisitStaging.csv')
        vRptDoctorShifts.to_csv('C:\\_BISolutions\\ETLFinal_YuanlongZhang\\DataFiles\\Staging\\vRptDoctorShiftsStaging.csv')

        MongoRptPatient = pd.read_csv('C:\\_BISolutions\\ETLFinal_YuanlongZhang\\DataFiles\\Staging\\vRptPatientVisitStaging.csv')
        MongoRptDoctor = pd.read_csv('C:\\_BISolutions\\ETLFinal_YuanlongZhang\\DataFiles\\Staging\\vRptDoctorShiftsStaging.csv')

        # 1. Whitelist your IP address and create a user on
        # ( https://account.mongodb.com/account/login )
        # 2. Create a connection string
        # Note: the connection string can change without notice!
        strCon = 'mongodb+srv://BICert:BICert@clinicreportsdata.ts9ek.mongodb.net/test?retryWrites=true&w=majority'

        objCon = pymongo.MongoClient(strCon)
        db = objCon["ClinicReportsData"]
        db.drop_collection('DoctorsShifts')
        objCol = db.create_collection('DoctorsShifts')
        MongoRptDoctor['_id'] = MongoRptDoctor['Unnamed: 0']
        MongoRptDoctor = MongoRptDoctor.drop(columns=['Unnamed: 0'])
        MongoRptDoctor = MongoRptDoctor.to_dict('records')
        objCol.insert_many(MongoRptDoctor)

        db.drop_collection('PatientsVisits')
        objCol = db.create_collection('PatientsVisits')
        MongoRptPatient['_id'] = MongoRptPatient['Unnamed: 0']
        MongoRptPatient = MongoRptPatient.drop(columns=['Unnamed: 0'])
        MongoRptPatient = MongoRptPatient.to_dict('records')
        objCol.insert_many(MongoRptPatient)

except Exception as e:
        print(e)
