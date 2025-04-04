#!/bin/bash

# Name this file run_all_incremental.sh and place it in the datagen folder

# Array of project directories
projects=("mssql" "mysql" "pgsql")

for project in "${projects[@]}"; do
    echo "Running incremental script for $project"
    
    if [ -d "project_$project" ]; then
        # Check if test_data_incremental.py exists in the project directory
        if [ -f "project_$project/test_data_incremental.py" ]; then
            # Run the Python script
            cd "project_$project"
            sh "run_incremental_$project.sh"
            cd ..
        else
            echo "test_data_incremental.py not found in $project"
        fi
    else
        echo "Project directory $project not found"
    fi
    
    echo "Finished processing $project"
    echo "----------------------------"
done

echo "All incremental scripts have been run"
