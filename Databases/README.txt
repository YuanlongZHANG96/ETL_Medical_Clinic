You can use the following coded to restore the source databases:

ALTER DATABASE [Patients] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [Patients] 
 FROM DISK = N'C:/_BISolutions/UWETLFinalYourNameHere/SourceDatabases/Patients.bak' 
 WITH RECOVERY, REPLACE;
ALTER DATABASE [Patients] SET MULTI_USER;
go
ALTER DATABASE [DoctorsSchedules] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
RESTORE DATABASE [DoctorsSchedules] 
 FROM DISK = N'C:/_BISolutions/UWETLFinalYourNameHere/SourceDatabases/DoctorsSchedules.bak' 
 WITH RECOVERY, REPLACE;
ALTER DATABASE [Patients] SET MULTI_USER;


Note: I have also included the Main Data Files (.mdf) to attach the databases if the backups fail. To use them you must:

1. Open SSMS as an Admin from the Start Menu.
2. Remove the Log Files (.ldf) in the bottom of the Attach Databases dialog window (in the database details pane).


