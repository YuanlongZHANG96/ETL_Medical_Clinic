--*************************************************************************--
-- Title: Testing the Reporting Views
-- Author: RRoot, Yuanlong Zhang
-- Desc: This file creates several ETL views used in Admin reports
-- Change Log: When,Who,What
-- 2018-02-07,RRoot,Created File
-- 2022-03-15, Yuanlong Zhang, Updated for the final project
--**************************************************************************--
Use DWClinicReportDataYuanlongZhang;


--Create views for SSIS Job
Create or Alter View vDWClinicReportDataYuanlongZhangETLJobHistory
As
Select Top 100000
 [JobName] = j.name 
,[StepName] = h.step_name
,[RunDateTime] = msdb.dbo.agent_datetime(run_date, run_time)
,[RunDurationSeconds] = h.run_duration
,[RunStatus] = iif(h.run_status = 1, 'Success', 'Failure')
From msdb.dbo.sysjobs as j 
  Inner Join msdb.dbo.sysjobhistory as h 
    ON j.job_id = h.job_id 
--Where j.enabled = 1 And j.name = 'ETLDWClinicReportData'
Order by JobName, RunDateTime desc;


--Create view for row count reports
Create or Alter View DWClinicReportDataRowCounts
As
With [RowCounts] -- Using a CTE to access the Top Command for the Order By statement in the view
As(
Select [SortCol] = 1, [TableName] = 'DimDates', [CurrentNumberOfRows] = Count(*) From [DimDates]
Union               
Select [SortCol] = 2, [TableName] = 'DimClinics', [CurrentNumberOfRows] = Count(*) From [DimClinics]
Union                
Select [SortCol] = 3, [TableName] = 'DimDoctors', [CurrentNumberOfRows] = Count(*) From [DimDoctors]
Union                
Select [SortCol] = 4, [TableName] = 'DimPatients', [CurrentNumberOfRows] = Count(*) From [DimPatients]
Union
Select [SortCol] = 5, [TableName] = 'DimProcedures', [CurrentNumberOfRows] = Count(*) From [DimProcedures]
Union
Select [SortCol] = 6, [TableName] = 'DimShifts', [CurrentNumberOfRows] = Count(*) From [DimShifts]
Union
Select [SortCol] = 7, [TableName] = 'FactDoctorShifts', [CurrentNumberOfRows] = Count(*) From [FactDoctorShifts]
Union  
Select [SortCol] = 8, [TableName] = 'FactVisits', [CurrentNumberOfRows] = Count(*) From [FactVisits]
Union                
Select [SortCol] = 9, [TableName] = 'ETLLog', [CurrentNumberOfRows] = Count(*) From [ETLLog]
) 
Select Top 100000 [SortCol],[TableName],[CurrentNumberOfRows]
  From [RowCounts]
  Order By [SortCol] asc; -- Use a sort column so it does not sort by table name.
go


Select [SortCol],[TableName],[CurrentNumberOfRows] From DWClinicReportDataRowCounts;
go


--EXEC MSDB.dbo.sp_purge_jobhistory;  
--Truncate Table DWClinicReportDataYuanlongZhang.dbo.ETLLog;

-- Code for ETL Processes Report -- 
-- Stored Procedure Logging
Select 
  ETLLogID,ETLDate,ETLTime,ETLAction,ETLLogMessage
From vETLLog
GO

go
EXEC msdb.dbo.sp_start_job N'ETLDWClinicReportData' ;  
Select * From vDWClinicReportDataYuanlongZhangETLJobHistory;

-- SQL Job Logging
Select 
 [JobName]
,[StepName]
,[RunDateTime]
,[RunDurationSeconds] 
,[RunStatus]
From vDWClinicReportDataYuanlongZhangETLJobHistory;



SELECT  ETLDate, ETLTime, ETLAction, ETLLogMessage
FROM vETLLog
WHERE (ETLAction = 'pETLDimDates') And ETLDate = (Select Max(ETLDate) From vETLLog)