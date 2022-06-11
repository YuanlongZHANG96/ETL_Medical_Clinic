--*************************************************************************--
-- Title: DWFinal
-- Author: Yuanlong Zhang
-- Desc: This file tests you knowlege on how to create a Incremental ETL process with SQL code
-- Change Log: When,Who,What
-- 2021-01-17,RRoot,Created File
-- 2022-01-24,Yuanlong Zhang, Completed File
-- 2022-03-07,Yuanlong Zhang, Updated the file for Final Projects

--**************************************************************************--
IF NOT EXISTS (SELECT 1 from sys.servers where name = 'continuumsql.westus2.cloudapp.azure.com')
BEGIN
  EXEC sp_addlinkedserver @server = 'continuumsql.westus2.cloudapp.azure.com'
  EXEC sp_addlinkedsrvlogin 'continuumsql.westus2.cloudapp.azure.com'
                         ,'false'
                         ,NULL
                         ,'BICert'
                         ,'BICert'
END

IF NOT EXISTS (SELECT 1 from sys.servers where name = 'is-root01.ischool.uw.edu\BI')
BEGIN
  EXEC sp_addlinkedserver @server = 'is-root01.ischool.uw.edu\BI'
  EXEC sp_addlinkedsrvlogin 'is-root01.ischool.uw.edu\BI'
                         ,'false'
                         ,NULL
                         ,'BICert'
                         ,'BICert'
END


USE [DWClinicReportDataYuanlongZhang];
go
SET NoCount ON;

--  Setup Logging Objects ----------------------------------------------------

If NOT Exists(Select * From sys.tables where Name = 'ETLLog')
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

/****** [dbo].[DimDates] ******/
Create or Alter Procedure pETLFillDimDates
/* Author: RRoot
** Desc: Inserts data Into DimDates
** Change Log: When,Who,What
** 20200117,RRoot,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/2000'
      Declare @EndDate datetime = '12/31/2030' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row Into the date dimension table for this date
	   -- If ((Select Count(*) From DimDates) = 0) 
       Insert Into DimDates 
       ( [FullDate],[FullDateName],[MonthID],[MonthName],[YearID],[YearName])
       Values ( 
         --Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        @DateInProcess -- [FullDate]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Year(@DateInProcess) -- [YearID] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimDates'
	       ,@ETLLogMessage = 'DimDates filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimDates'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
 Select * From vETLLog;
*/
go


/****** [dbo].[DimDoctors] ******/
go 
Create or Alter View vETLDimDoctors
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for DimDoctors
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
As
  Select
	    [DoctorID] = d.DoctorID
	   ,[DoctorFullName] = d.FirstName + ' ' + d.LastName
	   ,[DoctorEmailAddress] = d.EmailAddress
	   ,[DoctorCity] = TRIM(d.City)
	   ,[DoctorState] = CASE WHEN LEN(TRIM(d.State)) > 2 THEN '(ERROR) ' + TRIM(d.State) ELSE TRIM(d.State) END
	   ,[DoctorZip] = d.Zip
  From [continuumsql.westus2.cloudapp.azure.com].[DoctorsSchedules].[dbo].[Doctors] d
  UNION
  Select -1, 'No Doctor', 'N/A', 'N/A', 'N/A', 000000
go
/* Testing Code:
 Select * From vETLDimDoctors;
*/

go
Create or Alter Procedure pETLSyncDimDoctors
/* Author: Yuanlong Zhang
** Desc: Updates data in DimDoctors using the vETLDimDoctors view
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
AS
Begin
	Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Merge Into DimDoctors as t
     Using vETLDimDoctors as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
      On  t.DoctorID = s.DoctorID
     When Not Matched -- At least one column value does not match add a new row:
      Then
       Insert (DoctorID, DoctorFullName, DoctorEmailAddress,DoctorCity,DoctorState, DoctorZip)
        Values (s.DoctorID
			  ,s.DoctorFullName
              ,s.DoctorEmailAddress
              ,s.DoctorCity
              ,s.DoctorState
			  ,s.DoctorZip)
      When Matched 
	  AND t.DoctorEmailAddress <> s.DoctorEmailAddress 
	  OR t.DoctorCity <> s.DoctorCity
	  OR t.DoctorState <> s.DoctorState
	  OR t.DoctorZip <> s.DoctorZip-- If there is a row in the target (dim) table that is no longer in the source table
       Then -- indicate that row is no longer current
        Update 
         Set t.DoctorEmailAddress = s.DoctorEmailAddress
            ,t.DoctorCity = s.DoctorCity
			,t.DoctorState = s.DoctorState
			,t.DoctorZip = s.DoctorZip
	  When Not Matched by Source
	  Then
	   Update
	    Set t.DoctorFullName = iif(patindex('%(Deleted)%',[DoctorFullName]) > 0, [DoctorFullName], [DoctorFullName] + ' (Deleted)')    
    ;

    -- ETL Logging Code --
		Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimDoctors'
	       ,@ETLLogMessage = 'DimDoctors synced';
		Set @RC = +1
	End Try
	Begin Catch
		Declare @ErrorMessage nvarchar(1000) = Error_Message();
		Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimDoctors'
	     ,@ETLLogMessage = @ErrorMessage;
		Set @RC = -1
	End Catch
	Return @RC;
End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimDoctors;
 Print @Status;
 Select * From DimDoctors
*/

/****** [dbo].[DimClinics] ******/
go 
Create or Alter View vETLDimClinics
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for DimClinics
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
As
  Select
	    [ClinicID] = str(c.ClinicID)
	   ,[ClinicName] = C.ClinicName
	   ,[ClinicCity] = C.City
	   ,[ClinicState] = C.State
	   ,[ClinicZip] = C.Zip
  From [continuumsql.westus2.cloudapp.azure.com].[DoctorsSchedules].[dbo].[Clinics] c
go
/* Testing Code:
 Select * From vETLDimClinics;
*/


go
Create or Alter Procedure pETLSyncDimClinics
/* Author: Yuanlong Zhang
** Desc: Updates data in DimClinics using the vETLDimClinics view
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
AS
Begin
	Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Merge Into DimClinics as t
     Using vETLDimClinics as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
      On  t.ClinicID = s.ClinicID
     When Not Matched -- At least one column value does not match add a new row:
      Then
       Insert (ClinicID, ClinicName, ClinicCity,ClinicState,ClinicZip)
        Values (s.ClinicID
			  ,s.ClinicName
              ,s.ClinicCity
              ,s.ClinicState
              ,s.ClinicZip)
      When Matched 
	  AND t.ClinicName <> s.ClinicName 
	  OR t.ClinicCity <> s.ClinicCity
	  OR t.ClinicState <> s.ClinicState
	  OR t.ClinicZip <> s.ClinicZip-- If there is a row in the target (dim) table that is no longer in the source table
       Then -- indicate that row is no longer current
        Update 
         Set t.ClinicName = s.ClinicName
            ,t.ClinicCity = s.ClinicCity
			,t.ClinicState = s.ClinicState
			,t.ClinicZip = s.ClinicZip
	  When Not Matched by Source
	  Then
	   Update
	    Set ClinicName = iif(patindex('%(Deleted)%',[ClinicName]) > 0, [ClinicName], [ClinicName] + ' (Deleted)')    

    ;

    -- ETL Logging Code --
		Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimClinics'
	       ,@ETLLogMessage = 'DimClinics synced';
		Set @RC = +1
	End Try
	Begin Catch
		Declare @ErrorMessage nvarchar(1000) = Error_Message();
		Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimClinics'
	     ,@ETLLogMessage = @ErrorMessage;
		Set @RC = -1
	End Catch
	Return @RC;
End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimClinics;
 Print @Status;
 Select * From DimClinics
*/

/****** [dbo].[DimShifts] ******/
go 
Create or Alter View vETLDimShifts
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for DimShifts
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
As
  Select
	    [ShiftID] = s.ShiftID
	   ,[ShiftStart] = s.ShiftStart
	   ,[ShiftEnd] = s.ShiftEnd
  From [continuumsql.westus2.cloudapp.azure.com].[DoctorsSchedules].[dbo].[Shifts] s
go
/* Testing Code:
 Select * From vETLDimShifts;
*/


go
Create or Alter Procedure pETLSyncDimShifts
/* Author: Yuanlong Zhang
** Desc: Updates data in DimShifts using the vETLDimShifts view
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
AS
Begin
	Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Merge Into DimShifts as t
     Using vETLDimShifts as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
      On  t.ShiftID = s.ShiftID
     When Not Matched -- At least one column value does not match add a new row:
      Then
       Insert (ShiftID,ShiftStart,ShiftEnd)
        Values (s.ShiftID
			  ,s.ShiftStart
              ,s.ShiftEnd)
      When Matched 
	  AND t.ShiftStart <> s.ShiftStart 
	  OR t.ShiftEnd <> s.ShiftEnd
	  -- If there is a row in the target (dim) table that is no longer in the source table
       Then -- indicate that row is no longer current
        Update 
         Set t.ShiftStart = s.ShiftStart
            ,t.ShiftEnd = s.ShiftEnd
    ;

    -- ETL Logging Code --
		Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimShifts'
	       ,@ETLLogMessage = 'DimShifts synced';
		Set @RC = +1
	End Try
	Begin Catch
		Declare @ErrorMessage nvarchar(1000) = Error_Message();
		Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimShifts'
	     ,@ETLLogMessage = @ErrorMessage;
		Set @RC = -1
	End Catch
	Return @RC;
End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimShifts;
 Print @Status;
 Select * From DimShifts
*/


/****** [dbo].[FactDoctorShifts] ******/
go 
Create or Alter View vETLFactDoctorShifts
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for FactDoctorShifts
** Change Log: When,Who,What
** 2022-01-25,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for Final Projects.
*/
As
  SELECT [DoctorsShiftID] = DS.DoctorsShiftID
        ,[ShiftDateKey] = DK.DateKey
        ,[ClinicKey] = DC.ClinicID
        ,[ShiftKey] = DSS.ShiftKey
        ,[DoctorKey] = DD.DoctorID
        ,[HoursWorked] = CASE 
			WHEN DATEPART(hour,DSS.ShiftStart) < DATEPART(hour,DSS.ShiftEnd) 
			THEN DATEDIFF(hour,DSS.ShiftStart,DSS.ShiftEnd)
			ELSE DATEDIFF(hour,DSS.ShiftStart,DSS.ShiftEnd) + 24
			END
    FROM [continuumsql.westus2.cloudapp.azure.com].[DoctorsSchedules].[dbo].[DoctorShifts] as DS
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimDates as DK
     ON DS.ShiftDate = DK.FullDate
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimDoctors as DD
     ON DS.DoctorID = DD.DoctorID
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimClinics as DC
     ON DS.ClinicID = DC.ClinicID
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimShifts as DSS
     ON DS.ShiftID = DSS.ShiftID
go

/* Testing Code:
 Select * From vETLFactDoctorShifts;
*/


go
Create or Alter Procedure pETLSyncFactDoctorShifts
/* Author: Yuanlong Zhang
** Desc: Inserts data into FactDoctorShifts
** Change Log: When,Who,What
** 2022-01-25, Yuanlong Zhang, Created Sproc (MERGE).
** 2022-03-07, Yuanlong Zhang, Updated Sproc (MERGE) for final projects.
*/
As
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code£º  --
	Begin Tran;
		Merge Into FactDoctorShifts as t
		Using vETLFactDoctorShifts as s
			ON t.DoctorsShiftID = s.DoctorsShiftID
			AND t.ShiftDateKey = s.ShiftDateKey
			AND t.ClinicKey = s.ClinicKey
			AND t.ShiftKey = s.ShiftKey
			AND t.DoctorKey = s.DoctorKey
			When Not Matched 
				Then -- The Primary Key in Fact Table included 5 columns, when the combination of them in the Source is not found the the Target
				INSERT 
					VALUES ( s.DoctorsShiftID, s.ShiftDateKey, s.ClinicKey, s.ShiftKey, s.DoctorKey, s.HoursWorked )
			When Matched -- When the IDs match for the row currently being looked 
			AND  t.HoursWorked <> s.HoursWorked -- but the order quantity
				Then 
				UPDATE -- It know your target, so you dont specify the DimCustomers
					SET t.HoursWorked = s.HoursWorked
			When Not Matched By Source 
				Then -- The Primary Key is in the Target table, but not the source table
					DELETE
		; -- The merge statement demands a semicolon at the end!


	Commit Tran;
	Set @RC = +1
    Exec pInsETLLog
	        @ETLAction = 'pETLSyncFactDoctorShifts'
	       ,@ETLLogMessage = 'FactDoctorShifts Synced';
    Set @RC = 1;
   End try
   Begin catch
	 IF @@TranCount > 0 Rollback Tran;
     Declare @ErrorMessage nvarchar(1000) = Error_Message()
	 Exec pInsETLLog 
	      @ETLAction = 'pETLSyncFactDoctorShifts'
	     ,@ETLLogMessage = @ErrorMessage;
     Set @RC = -1;
   End Catch
   Return @RC;
 End
;
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncFactDoctorShifts;
 Print @Status;
 select * from factdoctorshifts
*/
go


/****** [dbo].[DimProcedures] ******/
go 
Create or Alter View vETLDimProcedures
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for DimProcedures
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
As
  Select
	    [ProcedureID] = p.ID
	   ,[ProcedureName] = p.[Name]
	   ,[ProcedureDesc] = p.[Desc]
	   ,[ProcedureCharge] = p.Charge
  From [is-root01.ischool.uw.edu\BI].[Patients].[dbo].[Procedures] p
go
/* Testing Code:
 Select * From vETLDimProcedures;
*/


go
Create or Alter Procedure pETLSyncDimProcedures
/* Author: Yuanlong Zhang
** Desc: Updates data in DimShifts using the vETLDimProcedures view
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
AS
Begin
	Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    Merge Into DimProcedures as t
     Using vETLDimProcedures as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
      On  t.ProcedureID = s.ProcedureID
     When Not Matched -- At least one column value does not match add a new row:
      Then
       Insert (ProcedureID, ProcedureName,ProcedureDesc, ProcedureCharge)
        Values (s.ProcedureID
			  ,s.ProcedureName
              ,s.ProcedureDesc
			  ,s.ProcedureCharge)
      When Matched 
	  AND t.ProcedureName <> s.ProcedureName 
	  OR t.ProcedureDesc <> s.ProcedureDesc
	  OR t.ProcedureCharge <> s.ProcedureCharge
	  -- If there is a row in the target (dim) table that is no longer in the source table
       Then -- indicate that row is no longer current
        Update 
         Set t.ProcedureName = s.ProcedureName
            ,t.ProcedureDesc = s.ProcedureDesc
			,t.ProcedureCharge = s.ProcedureCharge
	  When Not Matched by Source
	  Then
	   Update
	    Set ProcedureName = iif(patindex('%(Deleted)%',[ProcedureName]) > 0, [ProcedureName], [ProcedureName] + ' (Deleted)')    

    ;

    -- ETL Logging Code --
		Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimProcedures'
	       ,@ETLLogMessage = 'DimProcedures synced';
		Set @RC = +1
	End Try
	Begin Catch
		Declare @ErrorMessage nvarchar(1000) = Error_Message();
		Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimProcedures'
	     ,@ETLLogMessage = @ErrorMessage;
		Set @RC = -1
	End Catch
	Return @RC;
End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimProcedures;
 Print @Status;
 Select * From DimProcedures
*/


/****** [dbo].[DimPatients] ******/
go 
Create or Alter View vETLDimPatients
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for DimPatients - SCD-2
** Change Log: When,Who,What
** 2022-01-25,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
As
  Select [PatientID] = p.ID
        ,[PatientFullName] = Cast(p.FName+ ' ' + p.LName as nvarchar(100))
        ,[PatientCity] = p.City
        ,[PatientState] = p.State
        ,[PatientZipCode] = p.ZipCode
    FROM [is-root01.ischool.uw.edu\BI].[Patients].[dbo].[Patients] p
go
/* Testing Code:
 Select * From vETLDimPatients;
*/

go
Create or Alter Procedure pETLSyncDimPatients
/* Author: Yuanlong Zhang
** Desc: Inserts data into DimPatients  - Type 2 SCD
** Change Log: When,Who,What
** 2022-01-24,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final project.
*/
As
Begin
  Declare @RC int = 0;
	Begin Try
    -- ETL Processing Code --
    Merge Into DimPatients as t
     Using vETLDimPatients as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
      On  t.PatientID = s.PatientID
      And t.PatientFullName = s.PatientFullName
      And t.PatientCity = s.PatientCity
      And t.PatientState = s.PatientState
	  And t.PatientZipCode = s.PatientZipCode
     When Not Matched -- At least one column value does not match add a new row:
      Then
       Insert (PatientID, PatientFullName, PatientCity, PatientState, PatientZipCode, 
               StartDate, EndDate, IsCurrent)
        Values (s.PatientID
              ,s.PatientFullName
              ,s.PatientCity
              ,s.PatientState
			  ,s.PatientZipCode
              ,Cast(Convert(nvarchar(100), GetDate(), 112) as date) -- Smart Key can be joined to the DimDate
              ,Null
              ,1)
      When Not Matched By Source -- If there is a row in the target (dim) table that is no longer in the source table
       Then -- indicate that row is no longer current
        Update 
         Set t.EndDate = Cast(Convert(nvarchar(100), GetDate(), 112) as date) -- Smart Key can be joined to the DimDate
            ,t.IsCurrent = 0
    ;

    -- ETL Logging Code --
		Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimPatients'
	       ,@ETLLogMessage = 'DimPatients synced';
		Set @RC = +1
	End Try

	-- Error Handling Code --
	Begin Catch
		Declare @ErrorMessage nvarchar(1000) = Error_Message();

    -- ETL Logging Code --
		Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimPatients'
	     ,@ETLLogMessage = @ErrorMessage;
		Set @RC = -1
	End Catch
	Return @RC;
End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimPatients;
 Print @Status;
 select * from dimpatients
*/
go


/****** [dbo].[FactVisits] ******/
go 

Create or Alter View vETLFactVisits
/* Author: Yuanlong Zhang
** Desc: Extracts and transforms data for FactVisits
** Change Log: When,Who,What
** 2022-01-25,Yuanlong Zhang,Created Sproc (MERGE).
** 2022-03-07,Yuanlong Zhang,Created Sproc (MERGE) for final projects.
*/
As
  SELECT [VisitKey] = v.ID
        ,[DateKey] = DD.DateKey
        ,[ClinicKey] = DC.ClinicID
        ,[PatientKey] = v.Patient
        ,[DoctorKey] = CASE WHEN v.Doctor IS NULL THEN
		    (SELECT DoctorKey FROM DimDoctors where DoctorID = -1)
		    ELSE V.Doctor END
        ,[ProcedureKey] = v.[Procedure]
        ,[ProcedureVistCharge] = v.Charge
    FROM [is-root01.ischool.uw.edu\BI].[Patients].[dbo].[Visits] v
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimDates as DD
     ON cast(v.Date as date) = cast(DD.FullDate as date)
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimPatients as DP
     ON v.Patient = DP.PatientID and DP.IsCurrent = 1 -- Join with the current version of data
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimClinics as DC
     ON DC.ClinicID = V.Clinic/100
    JOIN DWClinicReportDataYuanlongZhang.dbo.DimProcedures as DPC
     ON v.[Procedure]=DPC.ProcedureID -- Join with the current version of data
	LEFT OUTER JOIN DWClinicReportDataYuanlongZhang.dbo.DimDoctors as DDC
	 ON v.Doctor = DDC.DoctorID
go

/* Testing Code:
 Select * From vETLFactVisits;
*/


go
Create or Alter Procedure pETLSyncFactVisits
/* Author: Yuanlong Zhang
** Desc: Inserts data into FactVisits
** Change Log: When,Who,What
** 2022-01-25, Yuanlong Zhang, Created Sproc (MERGE).
** 2022-03-07, Yuanlong Zhang, Created Sproc (MERGE) for final project.
*/
As
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code£º  --
	Begin Tran;
		Merge Into FactVisits as t
		Using vETLFactVisits as s
			ON t.VisitKey = s.VisitKey
			AND t.DateKey = s.DateKey
			AND t.ClinicKey = s.ClinicKey
			AND t.PatientKey = s.PatientKey
			AND t.DoctorKey = s.DoctorKey
			AND t.ProcedureKey = s.ProcedureKey
			When Not Matched 
				Then -- The Primary Key in Fact Table included 5 columns, when the combination of them in the Source is not found the the Target
				INSERT 
					VALUES ( s.VisitKey, s.DateKey, s.ClinicKey, s.PatientKey, s.DoctorKey, s.ProcedureKey, s.ProcedureVistCharge )
			When Matched -- When the IDs match for the row currently being looked 
			AND s.ProcedureVistCharge <> t.ProcedureVistCharge -- but the order quantity
			Then 
				UPDATE -- It know your target, so you dont specify the DimCustomers
					SET t.ProcedureVistCharge = s.ProcedureVistCharge
			When Not Matched By Source 
				Then -- The Primary Key is in the Target table, but not the source table
					DELETE
		; -- The merge statement demands a semicolon at the end!


	Commit Tran;
	Set @RC = +1
    Exec pInsETLLog
	        @ETLAction = 'pETLSyncFactVisits'
	       ,@ETLLogMessage = 'FactVisits Synced';
    Set @RC = 1;
   End try
   Begin catch
	 IF @@TranCount > 0 Rollback Tran;
     Declare @ErrorMessage nvarchar(1000) = Error_Message()
	 Exec pInsETLLog 
	      @ETLAction = 'pETLSyncFactVisits'
	     ,@ETLLogMessage = @ErrorMessage;
     Set @RC = -1;
   End Catch
   Return @RC;
 End
;
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncFactVisits;
 Print @Status;
 select * from FactVisits

*/
go

select * from ETLLog

--********************************************************************--
-- C)  NOT NEEDED FOR INCREMENTAL LOADING: Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--


--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int = 0;
Exec @Status = pETLSyncDimClinics;
Select [Object] = 'pETLSyncDimClinics', [Status] = @Status;

Exec @Status = pETLSyncDimDoctors;
Select [Object] = 'pETLSyncDimDoctors', [Status] = @Status;

Exec @Status = pETLSyncDimPatients;
Select [Object] = 'pETLSyncDimPatients', [Status] = @Status;

Exec @Status = pETLSyncDimProcedures;
Select [Object] = 'pETLSyncDimProcedures', [Status] = @Status;

Exec @Status = pETLSyncDimShifts;
Select [Object] = 'pETLSyncDimShifts', [Status] = @Status;

--Note: The dimdate table should be only run ONCE.
--After created DimDates table, you should comment out the command.
if (select count(*) from dbo.DimDates ) > 0 
   Begin
	  Select [Information] = 'Table DimDates already exists!';
   End
else
   Begin
      Exec @Status = pETLFillDimDates;
      Select [Object] = 'pETLFillDimDates', [Status] = @Status;
   End;

Exec @Status = pETLSyncFactDoctorShifts;
Select [Object] = 'pETLFillFactDoctorShifts', [Status] = @Status;

Exec @Status = pETLSyncFactVisits;
Select [Object] = 'pETLFillFactVisits', [Status] = @Status;
go


Select * from [dbo].[DimClinics]
Select * from [dbo].[DimDates]
Select * from [dbo].[DimDoctors]
Select * from [dbo].[DimPatients]
Select * from [dbo].[DimProcedures]
Select * from [dbo].[DimShifts]
Select * from [dbo].[FactDoctorShifts]
Select * from [dbo].[FactVisits]

--Test Code
--Select * from [dbo].[DimProducts] where ProductName = 'Test Ins Product';
--Select * from [dbo].[DimProducts] where ProductName = 'Test Upd Product' and iscurrent =1;
--Select * from [dbo].[DimCustomers] where CustomerFullName like '%Test Ins Customer%';
--Select * from [dbo].[DimCustomers] where CustomerFullName like '%Test Upd Customer%' and iscurrent =1;
--Select * from [dbo].[DimDates];
--Select * from [dbo].[FactSalesOrders] where SalesOrderID = 11111;

select * from dbo.ETLLog