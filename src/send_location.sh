#!/bin/bash

# Define source base path and stage path
source_base_path="/mnt/c/temp/LOCATION_FILES"  # Replace with your base source path
stage_path="@location_data"                    # Replace with your Snowflake stage

# Ensure the stage path ends with a slash
stage_path="${stage_path%/}/"

# Loop through each subfolder in the source base path
for subfolder in "$source_base_path"/*; do
    # Check if the subfolder name matches the format YYYY_MM_DD
    if [[ "$(basename "$subfolder")" =~ ^[0-9]{4}_[0-9]{2}_[0-9]{2}$ ]]; then
        echo "Processing subfolder: $subfolder"

        # Loop through each .csv file in the valid subfolder
        for filepath in "$subfolder"/*.csv; do
            # Check if the file exists (in case the folder is empty)
            if [ -e "$filepath" ]; then
                # Get the base name of the file
                filename=$(basename "$filepath")

                # Construct the target path in the stage
                target_path="$stage_path$(basename "$subfolder")/"

                # Execute the PUT command to load the file into Snowflake
                echo "PUT file://$filepath $target_path;"
                snowsql -q "PUT file://$filepath $target_path;"
            else
                echo "No .csv files found in $subfolder"
            fi
        done
    else
        echo "Skipping non-matching subfolder: $subfolder"
    fi
done
