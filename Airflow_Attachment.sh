#!/bin/bash

# Capture the start time of the script
SCRIPT_START_TIME=$(date +%Y-%m-%dT%H:%M:%S.%3NZ)

# Airflow instance and credentials
AIRFLOW_INSTANCE="http://35.93.48.46:8080"
USER=op_mguser
PASS=Test@123

# ServiceNow credentials and URL
SERVICENOW_INSTANCE="https://dev255231.service-now.com"
SERVICENOW_USER="testuser"
SERVICENOW_PASS="mgOIP@2024"

# API CALLS - Incident & Attachment
SERVICENOW_API="/api/now/table/incident"
SERVICENOW_ATTACHMENT_API="/api/now/attachment/file"

# Input JSON file containing DAG names
INPUT_FILE="/home/metric_script/dag_id.json"
OUTPUT_FILE="/home/metric_logs/Airflow_Metrics.log"

# Print the contents of the JSON file for debugging
echo "Processing the contents of ${INPUT_FILE}"
#cat ${INPUT_FILE}

# Read the DAG names from the input JSON file
DAG_NAMES=$(jq -r '.Dag_id[]?' ${INPUT_FILE})

# Initialize arrays for paused and unpaused DAGs
declare -a UNPAUSED_DAGS=()

# Function to check if a DAG is paused
check_dag_status() {
    local TARGET_DAG=$1

    # Fetch DAG details for the specified DAG
    DAG_STATE=$(curl -s -X GET ${AIRFLOW_INSTANCE}/api/v1/dags/${TARGET_DAG} -H 'content-type: application/json' --user ${USER}:${PASS} | jq -r '.is_paused')

    if [ "${DAG_STATE}" == "false" ]; then
        UNPAUSED_DAGS+=("${TARGET_DAG}")
    fi
}

# Check the status of each DAG
for DAG_NAME in ${DAG_NAMES}; do
    check_dag_status "${DAG_NAME}"
done

# Initialize an empty array to collect the details
declare -a dag_run_details=()

# Function to fetch the latest DAG run details
fetch_latest_dag_run_details() {
    local TARGET_DAG=$1

    # Fetch DAG runs for the specified DAG
    DAG_DETAILS=$(curl -s -X GET ${AIRFLOW_INSTANCE}/api/v1/dags/${TARGET_DAG}/dagRuns -H 'content-type: application/json' --user ${USER}:${PASS})

    # Sort the DAG runs by execution_date and get the latest one
    LATEST_DAG_RUN=$(echo ${DAG_DETAILS} | jq '.dag_runs | sort_by(.execution_date) | last')

    RUN_ID=$(echo ${LATEST_DAG_RUN} | jq -j .dag_run_id)
    START_TIME=$(echo ${LATEST_DAG_RUN} | jq -j .start_date)
    END_TIME=$(echo ${LATEST_DAG_RUN} | jq -j .end_date)
    STATUS=$(echo ${LATEST_DAG_RUN} | jq -j .state)

    # Format the start and end times
    FORMATTED_START_TIME=$(date --date "${START_TIME}" +%Y-%m-%dT%H:%M:%S.%3NZ 2>/dev/null)
    FORMATTED_END_TIME=$(date --date "${END_TIME}" +%Y-%m-%dT%H:%M:%S.%3NZ 2>/dev/null)

    # Initialize an empty string to collect error logs
    ERROR_LOGS=""
    ERROR_LOGS1=""

    # Fetch the task instances for the latest DAG run
    TASK_INSTANCES=$(curl -s -X GET "${AIRFLOW_INSTANCE}/api/v1/dags/${TARGET_DAG}/dagRuns/${RUN_ID}/taskInstances" -H 'content-type: application/json' --user "${USER}:${PASS}")

    # Parse task instances correctly
    TASK_IDS=$(echo "${TASK_INSTANCES}" | jq -r '.task_instances[]?.task_id')

    for TASK_ID in ${TASK_IDS}; do
        # Fetch the task instance log if the task failed
        TASK_STATUS=$(echo "${TASK_INSTANCES}" | jq -r ".task_instances[] | select(.task_id == \"${TASK_ID}\") | .state")
        if [ "${TASK_STATUS}" == "failed" ]; then
            TASK_LOG=$(curl -s -X GET "${AIRFLOW_INSTANCE}/api/v1/dags/${TARGET_DAG}/dagRuns/${RUN_ID}/taskInstances/${TASK_ID}/logs/1" -H 'content-type: application/json' --user "${USER}:${PASS}")
            if [ $? -ne 0 ]; then
                echo "Failed to fetch task instance log for DAG: ${TARGET_DAG}, Run ID: ${RUN_ID}, Task ID: ${TASK_ID}"
            fi

            # Save the task logs to a file
            ERROR_LOG_FILE="Error_Log_${TARGET_DAG}_${RUN_ID}_${TASK_ID}.txt"
            echo "${TASK_LOG}" > "${ERROR_LOG_FILE}"

            # Fetch the task instance log and filter for errors
            ERROR_LOGS1=$(echo "${TASK_LOG}" | grep "} ERROR -" | grep -v "Task failed" | sed 's/.*} ERROR - //' )
            if [ $? -ne 0 ]; then
                echo "Failed to fetch task instance log for DAG: ${TARGET_DAG}, Run ID: ${RUN_ID}, Task ID: ${TASK_ID}"
            fi

            # Collect the error logs
            ERROR_LOGS="${ERROR_LOGS1}"

            # Create a ServiceNow incident for the failed task
            DESCRIPTION="Dag_Name: ${TARGET_DAG} DAG_Run_ID: ${RUN_ID}
            Task ID: ${TASK_ID}
            Start Time: ${FORMATTED_START_TIME}
            End Time: ${FORMATTED_END_TIME}
            Error: ${ERROR_LOGS1}"

            INCIDENT_PAYLOAD=$(jq -n --arg short_description "Airflow DAG ${TARGET_DAG} Task ${TASK_ID} Failed" --arg description "$DESCRIPTION" --arg category "pipeline" --arg caller_id "administrator" --arg work_notes "Error log attached." '{short_description: $short_description, description: $description, category: $category, caller_id: $caller_id, work_notes: $work_notes}')
            RESPONSE=$(curl -s -w "%{http_code}" -o response.json -X POST "${SERVICENOW_INSTANCE}${SERVICENOW_API}" \
                -H "Content-Type: application/json" \
                --user "${SERVICENOW_USER}:${SERVICENOW_PASS}" \
                -d "${INCIDENT_PAYLOAD}")

            HTTP_CODE=$(echo "${RESPONSE}" | tail -n 1)
            if [ "$HTTP_CODE" -ne 201 ]; then
                echo "Failed to create ServiceNow incident for DAG: ${TARGET_DAG}, Run ID: ${RUN_ID}, Task ID: ${TASK_ID}. HTTP Response Code: ${HTTP_CODE}"
            else
                echo "Successfully created ServiceNow incident for DAG: ${TARGET_DAG}, Run ID: ${RUN_ID}, Task ID: ${TASK_ID}"
                INCIDENT_SYS_ID=$(jq -r '.result.sys_id' response.json)
                # Attach the error log to the incident
                ATTACHMENT_RESPONSE=$(curl -s -w "%{http_code}" -o /dev/null -X POST "${SERVICENOW_INSTANCE}${SERVICENOW_ATTACHMENT_API}?table_name=incident&table_sys_id=${INCIDENT_SYS_ID}&file_name=${ERROR_LOG_FILE}" \
                    --user "${SERVICENOW_USER}:${SERVICENOW_PASS}" \
                    -F "uploadFile=@${ERROR_LOG_FILE}")

                ATTACHMENT_HTTP_CODE=$(echo "${ATTACHMENT_RESPONSE}" | tail -n 1)
                if [ "$ATTACHMENT_HTTP_CODE" -ne 201 ]; then
                    echo "Failed to attach error log to ServiceNow incident for DAG: ${TARGET_DAG}, Run ID: ${RUN_ID}, Task ID: ${TASK_ID}. HTTP Response Code: ${ATTACHMENT_HTTP_CODE}"
                else
                    echo "Successfully attached error log to ServiceNow incident for DAG: ${TARGET_DAG}, Run ID: ${RUN_ID}, Task ID: ${TASK_ID}"
                fi
            fi
        fi
    done

    # Collect the details in the array
    dag_run_details+=("{\"index\":\"airflow\", \"Pipeline_Name\":\"${TARGET_DAG}\", \"Run_ID\":\"${RUN_ID}\", \"Start_Time\":\"${FORMATTED_START_TIME}\", \"End_Time\":\"${FORMATTED_END_TIME}\", \"Status\":\"${STATUS}\", \"Error_Description\":\"${ERROR_LOGS}\"}")
}

# Fetch the latest DAG run details for each unpaused DAG
for DAG in "${UNPAUSED_DAGS[@]}"; do
    fetch_latest_dag_run_details "${DAG}"
done

# Write the collected details to the JSON file
{
    IFS=$'\n' # Change the IFS to newline
    if [ ${#dag_run_details[@]} -ne 0 ]; then
        for entry in "${dag_run_details[@]}"; do
            echo "$entry"
        done > ${OUTPUT_FILE}
    else
        echo "[]" > ${OUTPUT_FILE}
    fi
}

# Capture the end time of the script
SCRIPT_END_TIME=$(date +%Y-%m-%dT%H:%M:%S.%3NZ)

# Calculate the total duration of the script
SCRIPT_DURATION=$(($(date -d "${SCRIPT_END_TIME}" +%s) - $(date -d "${SCRIPT_START_TIME}" +%s)))

# Display the start time, end time, and duration of the script
echo "Script Start Time: ${SCRIPT_START_TIME}"
echo "Script End Time: ${SCRIPT_END_TIME}"
echo "Script Duration: ${SCRIPT_DURATION} seconds"
echo "Pipeline details written to ${OUTPUT_FILE}"
