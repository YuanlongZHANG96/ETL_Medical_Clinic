--*************************************************************************--
-- Title: DWFinal-DWMongoDBView
-- Author: Yuanlong Zhang
-- Desc: This file used to create view to feed MongoDB
-- Change Log: When,Who,What
-- 2021-01-17,RRoot,Created File
-- 2022-01-24,Yuanlong Zhang, Completed File
-- 2022-03-10,Yuanlong Zhang, Updated the file for Final Projects

--**************************************************************************--

USE DWClinicReportDataYuanlongZhang;
GO

IF (Object_ID('vRptDoctorShifts') is not null) Drop View vRptDoctorShifts;
GO
Create View vRptDoctorShifts
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
ON fds.DoctorKey = ddo.DoctorKey

select * from vRptDoctorShifts



IF (Object_ID('vRptPatientVisits') is not null) Drop View vRptPatientVisits;
GO;

Create View vRptPatientVisits
AS
Select 
[VisitDate] = Cast(Cast([FullDate] as date) as varchar(100))
,[ClinicName] = dc.ClinicName
,[ClinicCity] = dc.ClinicCity
,[ClinicState] = dc.ClinicState
,[ProcedureName] = dp.ProcedureName
,[ProcedureDesc] = dp.ProcedureDesc
,[PatientFullName] = dpt.PatientFullName
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
ON fv.PatientKey = dpt.PatientID

select * from vRptPatientVisits