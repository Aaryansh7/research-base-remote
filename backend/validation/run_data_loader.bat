@echo off
setlocal

:: Define batch size
SET BATCH_SIZE=100

:: Initialize start and end indices
SET START_INDEX=0
SET END_INDEX=%BATCH_SIZE%

:: Define a log file for detailed output
SET LOG_FILE="dataloader_run.log"

echo Starting data loading process. Logs will be written to %LOG_FILE%
echo -------------------------------------------------- >> %LOG_FILE%
echo Starting data loading process. Logs will be written to %LOG_FILE%

:loop_start
    echo Attempting to process batch: [%START_INDEX%:%END_INDEX%]
    echo Attempting to process batch: [%START_INDEX%:%END_INDEX%] >> %LOG_FILE%
    
    :: Add a loading message before executing the Python script
    echo Processing batch, please wait...
    echo Processing batch, please wait... >> %LOG_FILE%
    
    :: Execute the Python script with current batch indices
    :: Redirect stdout and stderr to the log file
    python bash_getallcompanydata.py --start_index %START_INDEX% --end_index %END_INDEX% >> %LOG_FILE% 2>&1
    
    :: Capture the exit status of the Python script
    SET PYTHON_EXIT_STATUS=%ERRORLEVEL%

    IF %PYTHON_EXIT_STATUS% EQU 0 (
        echo Python script completed successfully for batch [%START_INDEX%:%END_INDEX%].
        echo Python script completed successfully for batch [%START_INDEX%:%END_INDEX%]. >> %LOG_FILE%
        
        :: Increment indices for the next batch
        SET /A START_INDEX=%START_INDEX% + %BATCH_SIZE%
        SET /A END_INDEX=%END_INDEX% + %BATCH_SIZE%

        :: Add a small delay between successful batches
        timeout /t 5 /nobreak >NUL

    ) ELSE IF %PYTHON_EXIT_STATUS% EQU 1 (
        echo Python script exited with an error (status 1) for batch [%START_INDEX%:%END_INDEX%]. Retrying this batch...
        echo Python script exited with an error (status 1) for batch [%START_INDEX%:%END_INDEX%]. Retrying this batch... >> %LOG_FILE%
        :: The script will retry the same batch in the next iteration of the loop.
        :: Add a delay before retrying
        timeout /t 10 /nobreak >NUL
    ) ELSE (
        echo Python script exited with an unexpected status: %PYTHON_EXIT_STATUS% for batch [%START_INDEX%:%END_INDEX%].
        echo Python script exited with an unexpected status: %PYTHON_EXIT_STATUS% for batch [%START_INDEX%:%END_INDEX%]. >> %LOG_FILE%
        echo Exiting script.
        echo Exiting script. >> %LOG_FILE%
        GOTO :eof :: Exit the batch script
    )

    GOTO :loop_start :: Continue to the next iteration

:eof
echo -------------------------------------------------- >> %LOG_FILE%
echo Data loading process finished. >> %LOG_FILE%
echo --------------------------------------------------
echo Data loading process finished.
endlocal
