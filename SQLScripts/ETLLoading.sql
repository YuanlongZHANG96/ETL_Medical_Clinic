--*************************************************************************--
-- Title: DWFinal-ETL Loading
-- Author: Yuanlong Zhang
-- Desc: This file used to load the new data into the database
-- Change Log: When,Who,What
-- 2021-01-17,RRoot,Created File
-- 2022-01-24,Yuanlong Zhang, Completed File
-- 2022-03-01,Yuanlong Zhang, Updated the file for Final Projects

--**************************************************************************--


USE Patients;
go

--  Setup Logging Objects ----------------------------------------------------

If NOT Exists(Select * From Sys.tables where Name = 'ETLLog')
  Create -- Drop
  Table ETLLog
  (ETLLogID int identity Primary Key
  ,ETLDateAndTime datetime Default GetDate()
  ,ETLAction varchar(100)
  ,ETLLogMessage varchar(2000)
  );
go

Create or Alter View vETLLog
As
  Select
   ETLLogID
  ,ETLDate = Format(ETLDateAndTime, 'D', 'en-us')
  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), 'HH:mm', 'en-us')
  ,ETLAction
  ,ETLLogMessage
  From ETLLog;
go


Create or Alter Proc pInsETLLog
 (@ETLAction varchar(100), @ETLLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created Sproc
--*************************************************************************--
As
Begin
  Declare @RC int = 0;
  Begin Try
    Begin Tran;
      Insert Into ETLLog
       (ETLAction,ETLLogMessage)
      Values
       (@ETLAction,@ETLLogMessage)
    Commit Tran;
    Set @RC = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @RC = -1;
  End Catch
  Return @RC;
End
Go


--********************************************************************--
-- A) Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
 -- NOT NEEDED FOR INCREMENTAL LOADING: 
--********************************************************************--


--********************************************************************--
-- B) Synchronize the Tables
--********************************************************************--

/****** [dbo].[DimCustomers] ******/
go 
Create or Alter View vETLNewPatient
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 2022-01-25,Yuanlong Zhang,Created Sproc (MERGE).
*/
As
  Select [FName] = [FName]
        ,[LName] = [LName]
        ,[Email] = [Email]
        ,[Address] = [Address]
        ,[City] = [City]
        ,[State] = [State]
		,[ZipCode] = [ZipCode]
    FROM [Staging].[dbo].[StagingNewPatient]
go
/* Testing Code:
 SELECT * FROM vETLNewPatient;
*/



go
Create or Alter Procedure pETLSyncPatients
/* Author: Yuanlong Zhang
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
*/
As
Begin
  Declare @RC int = 0;
	Begin Try
    -- ETL Processing Code --
    Merge Into Patients as t
     Using vETLNewPatient as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
      On  t.FName = s.FName
      And t.LName = s.LName
      And t.Email = s.Email
     When Not Matched -- At least one column value does not match add a new row:
      Then
       Insert (FName, LName, Email, Address, City, State, ZipCode)
        Values (s.FName
              ,s.LName
              ,s.Email
              ,s.Address
			  ,s.City
			  ,s.State
              ,s.ZipCode)
      When Matched 
	  AND (t.Address <> s.Address OR t.City <> s.City OR t.State <> s.State OR t.ZipCode <> s.ZipCode)
	  -- If there is a row in the target (dim) table that is no longer in the source table
       Then -- indicate that row is no longer current
        Update 
         Set t.Address = s.Address
            ,t.City = s.City
			,t.State = s.State
			,t.ZipCode = s.ZipCode
    ;

    -- ETL Logging Code --
		Exec pInsETLLog
	        @ETLAction = 'pETLSyncPatients'
	       ,@ETLLogMessage = 'Patients synced';
		Set @RC = +1
	End Try

	-- Error Handling Code --
	Begin Catch
		Declare @ErrorMessage nvarchar(1000) = Error_Message();

    -- ETL Logging Code --
		Exec pInsETLLog 
	      @ETLAction = 'pETLSyncPatients'
	     ,@ETLLogMessage = @ErrorMessage;
		Set @RC = -1
	End Catch
	Return @RC;
End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimCustomers;
 Print @Status;
*/
go

/*
go
Declare @Status int = 0;
Exec @Status = pETLSyncPatients;
Select [Object] = 'pETLSyncPatients', [Status] = @Status;
select * from dbo.ETLLog

select * from Patients.dbo.Patients
--WHERE fname = 'Michael'
*/



/****** [dbo].[FactOrders] ******/
go 
Create or Alter View vETLVisits
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 2022-01-25,Yuanlong Zhang,Created Sproc (MERGE).
*/
As
  SELECT [Date] = CAST(CAST(CONCAT(CAST(CONVERT(DATE,CONVERT(CHAR(8),SV.Date),112) AS VARCHAR), ' ', CAST(SV.Time AS VARCHAR)) AS DATETIME2) AS DATETIME) 
        ,[Clinic] = CLN.ID
        ,[Patient] = SV.Patient
        ,[Doctor] = SV.Doctor
        ,[Procedure] = SV.[Procedure]
        ,[Charge] = Cast(SV.Charge as decimal(18,2))
    FROM Staging.dbo.StagingVisits as SV
    JOIN [Patients].[dbo].[Clinics] as CLN
     ON SV.Clinic = CLN.City
    JOIN [Patients].[dbo].[Doctors] as DOC
	 ON DOC.ID = SV.Doctor
    JOIN [Patients].[dbo].[Patients] as PAT
     ON PAT.ID = SV.Patient
    JOIN [Patients].[dbo].[Procedures] as PRO
     ON PRO.ID = SV.[Procedure]
go

/* Testing Code:
 Select * From vETLVisits;
*/

go
Create or Alter Procedure pETLSyncVisits
/* Author: Yuanlong Zhang
** Desc: Inserts data into FactOrders
** Change Log: When,Who,What
** 2022-01-25, Yuanlong Zhang, Created Sproc (MERGE).
*/
As
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code£º  --
	Begin Tran;
		Merge Into Visits as t
		Using vETLVisits as s
			ON t.Date = s.Date
			AND t.Clinic = s.Clinic
			AND t.Patient = s.Patient
			AND t.Doctor = s.Doctor
			AND t.[Procedure] = s.[Procedure]
			When Not Matched 
				Then -- The Primary Key in Fact Table included 5 columns, when the combination of them in the Source is not found the the Target
				INSERT 
					VALUES ( s.Date, s.Clinic, s.Patient, s.Doctor, s.[Procedure], s.Charge )
		; -- The merge statement demands a semicolon at the end!

	Commit Tran;
	Set @RC = +1
    Exec pInsETLLog
	        @ETLAction = 'pETLSyncVisits'
	       ,@ETLLogMessage = 'Visits Synced';
    Set @RC = 1;
   End try
   Begin catch
	 IF @@TranCount > 0 Rollback Tran;
     Declare @ErrorMessage nvarchar(1000) = Error_Message()
	 Exec pInsETLLog 
	      @ETLAction = 'pETLSyncVisits'
	     ,@ETLLogMessage = @ErrorMessage;
     Set @RC = -1;
   End Catch
   Return @RC;
 End
;
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncVisits;
 Print @Status;
*/
go

/*
select * from Visits
*/
