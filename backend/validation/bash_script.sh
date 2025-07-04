#!/bin/bash

# Enable job control and allow background processes (for timeout if needed, though sleep is simpler)
set -m

# Define batch size
BATCH_SIZE=10

# Initialize start and end indices
START_INDEX=0
END_INDEX=$BATCH_SIZE

# Define a log file for detailed output
LOG_FILE="dataloader_run.log"

echo "Starting data loading process. Logs will be written to ${LOG_FILE}" | tee -a "${LOG_FILE}"
echo "--------------------------------------------------" | tee -a "${LOG_FILE}"
echo "Starting data loading process. Logs will be written to ${LOG_FILE}" | tee -a "${LOG_FILE}"

# Loop start
while true; do
    echo "Attempting to process batch: [${START_INDEX}:${END_INDEX}]" | tee -a "${LOG_FILE}"
    
    echo "Processing batch, please wait..." | tee -a "${LOG_FILE}"
    
    # Execute the Python script with current batch indices
    # Python script is assumed to handle its own logging to LOG_FILE
    python bash_getallcompanydata.py --start_index "${START_INDEX}" --end_index "${END_INDEX}"
    
    # Capture the exit status of the Python script
    PYTHON_EXIT_STATUS=$?
    echo "PYTHON Exit Status: ${PYTHON_EXIT_STATUS}" | tee -a "${LOG_FILE}"

    if [ "${PYTHON_EXIT_STATUS}" -eq 0 ]; then
        # Add a delay after execution (simulates timeout /t 10)
        sleep 10
        echo "Python script completed successfully for batch [${START_INDEX}:${END_INDEX}]." | tee -a "${LOG_FILE}"
        
        # Increment indices for the next batch
        START_INDEX=$((START_INDEX + BATCH_SIZE))
        END_INDEX=$((END_INDEX + BATCH_SIZE))

        # Add a small delay between successful batches (simulates timeout /t 5)
        sleep 5
        # Continue to the next iteration of the while loop
    elif [ "${PYTHON_EXIT_STATUS}" -eq 1 ]; then
        echo "Python script exited with an error (status 1) for batch [${START_INDEX}:${END_INDEX}]. Retrying this batch..." | tee -a "${LOG_FILE}"
        # Add a delay before retrying (simulates timeout /t 10)
        sleep 10
        # The loop will automatically retry the same batch
    else
        echo "Unexpected Python script exit status (${PYTHON_EXIT_STATUS}) for batch [${START_INDEX}:${END_INDEX}]. Terminating process." | tee -a "${LOG_FILE}"
        break # Exit the while loop
    fi
done

echo "--------------------------------------------------" | tee -a "${LOG_FILE}"
echo "Data loading process finished." | tee -a "${LOG_FILE}"
echo "--------------------------------------------------" | tee -a "${LOG_FILE}"

exit 0