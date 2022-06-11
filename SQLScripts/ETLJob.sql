--*************************************************************************--
-- Title: Create the DW ETL Job
-- Desc:This file will drop and create a SQL Agent Job 
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created File
-- 2022-03-14,Yuanlong Zhang, Updated file for Final Project
--*************************************************************************--


USE [master]
GO
Begin Try
-- Access to the Server
CREATE LOGIN [DESKTOP-AO8N3T1\i_ecn] 
 FROM WINDOWS 
  WITH DEFAULT_DATABASE=[master], DEFAULT_LANGUAGE=[us_english]

ALTER SERVER ROLE [sysadmin] ADD MEMBER [DESKTOP-AO8N3T1\i_ecn]
End Try
Begin Catch
	Print Error_Message()
End Catch
GO

-- Abstaction layer to the Login
Begin Try
CREATE CREDENTIAL [CredentialForETLAutomations] 
 WITH IDENTITY = N'DESKTOP-AO8N3T1\i_ecn'
End Try
Begin Catch
	Print Error_Message()
End Catch
GO

-- Connection to an account with enough permission 
-- using the abstraction layer credentials
Begin Try
	EXEC msdb.dbo.sp_add_proxy 
	 @proxy_name=N'SSIS Proxy'
	,@credential_name=N'CredentialForETLAutomations'
	,@enabled=1

	-- Map to SSIS subsystems
	EXEC msdb.dbo.sp_grant_proxy_to_subsystem 
	 @proxy_name=N'SSIS Proxy'
	,@subsystem_id=11  -- SSIS Package
End Try
Begin Catch
	Print Error_Message()
End Catch
GO


USE [msdb]
GO
BEGIN TRY
  IF Exists (Select * from SysJobs Where Name = 'ETLDWClinicReportData')
    Begin 
      Exec sp_delete_job @job_name = ETLDWClinicReportData
    End

  /****** Object:  Job [DWClinicReportDataYuanlongZhang]    Script Date: 8/21/2021 3:46:14 PM ******/
  BEGIN TRANSACTION
  DECLARE @ReturnCode INT
  SELECT @ReturnCode = 0
  /****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 8/21/2021 3:46:14 PM ******/
  IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
  BEGIN
  EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

  END

  DECLARE @jobId BINARY(16)
  EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'ETLDWClinicReportData', 
		  @enabled=1, 
		  @notify_level_eventlog=0, 
		  @notify_level_email=0, 
		  @notify_level_netsend=0, 
		  @notify_level_page=0, 
		  @delete_level=0, 
		  @description=N'Performs ETL tasks for ETLDWClinicReportData', 
		  @category_name=N'[Uncategorized (Local)]', 
		  @owner_login_name=N'sa', @job_id = @jobId OUTPUT
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
  /****** Object:  Step [Run DWIndependentBookSellersETLpackage.dtsx]    Script Date: 8/21/2021 3:46:14 PM ******/
  EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Run ETLJob.dtsx', 
		  @step_id=1, 
		  @cmdexec_success_code=0, 
		  @on_success_action=1, 
		  @on_success_step_id=0, 
		  @on_fail_action=2, 
		  @on_fail_step_id=0, 
		  @retry_attempts=0, 
		  @retry_interval=0, 
		  @os_run_priority=0, @subsystem=N'SSIS', 
		  @command=N'/FILE "C:\_BISolutions\ETLFinal_YuanlongZhang\ETLPackages\ETLJob.dtsx" /CHECKPOINTING OFF /REPORTING E', 
		  @database_name=N'master', 
		  @flags=0, 
		  @proxy_name=N'SSIS Proxy'
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
  EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
  EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'EachNight', 
		  @enabled=1, 
		  @freq_type=4, 
		  @freq_interval=1, 
		  @freq_subday_type=1, 
		  @freq_subday_interval=0, 
		  @freq_relative_interval=0, 
		  @freq_recurrence_factor=0, 
		  @active_start_date=20210821, 
		  @active_end_date=99991231, 
		  @active_start_time=10000, 
		  @active_end_time=235959, 
		  @schedule_uid=N'ac7412ed-e42f-46a0-a8bb-d16ccf0310fb'
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
  EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
  COMMIT TRANSACTION
  GOTO EndSave
  QuitWithRollback:
      IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
  EndSave:

END TRY
BEGIN CATCH
  IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
  Print Error_Message()
END CATCH

GO
