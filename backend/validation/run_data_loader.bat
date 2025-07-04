@echo off
setlocal ENABLEDELAYEDEXPANSION

SET BATCH_SIZE=100


SET START_INDEX=0

SET END_INDEX=!BATCH_SIZE! 

SET LOG_FILE="dataloader_run.log"


echo Starting data loading process. Logs will be written to %LOG_FILE%
echo -------------------------------------------------- >> "%LOG_FILE%"
echo Starting data loading process. Logs will be written to %LOG_FILE%

:loop_start
    echo Attempting to process batch: [!START_INDEX!:!END_INDEX!]
    echo Attempting to process batch: [!START_INDEX!:!END_INDEX!] >> "%LOG_FILE%"
    

    echo Processing batch, please wait...
    echo Processing batch, please wait... >> "%LOG_FILE%"
    

    python bash_getallcompanydata.py --start_index !START_INDEX! --end_index !END_INDEX!
    

    SET PYTHON_EXIT_STATUS=!ERRORLEVEL!
    echo PYTHON Exit Status: !PYTHON_EXIT_STATUS!
    echo PYTHON Exit Status: !PYTHON_EXIT_STATUS! >> "%LOG_FILE%"

    IF !PYTHON_EXIT_STATUS! EQU 0 (
        timeout /t 10 /nobreak >NUL
        echo Python script completed successfully for batch [!START_INDEX!:!END_INDEX!].
        echo Python script completed successfully for batch [!START_INDEX!:!END_INDEX!]. >> "%LOG_FILE%"
        
        SET /A START_INDEX=!START_INDEX! + !BATCH_SIZE!
        SET /A END_INDEX=!END_INDEX! + !BATCH_SIZE!

        timeout /t 5 /nobreak >NUL
        GOTO :loop_start
    ) ELSE (
        echo Python script exited with a non-zero error status (!PYTHON_EXIT_STATUS!) for batch [!START_INDEX!:!END_INDEX!].
        echo Python script exited with a non-zero error status (!PYTHON_EXIT_STATUS!) for batch [!START_INDEX!:!END_INDEX!]. >> "%LOG_FILE%"
        
        IF !PYTHON_EXIT_STATUS! EQU 1 (
            echo Retrying this batch...
            echo Retrying this batch... >> "%LOG_FILE%"
            timeout /t 10 /nobreak >NUL
            GOTO :loop_start
        ) ELSE (
            echo Unexpected Python script exit status. Terminating process.
            echo Unexpected Python script exit status. Terminating process. >> "%LOG_FILE%"
            GOTO :eof
        )
    )
    
GOTO :eof 

:eof
echo -------------------------------------------------- >> "%LOG_FILE%"
echo Data loading process finished. >> "%LOG_FILE%"
echo --------------------------------------------------
echo Data loading process finished.
endlocal