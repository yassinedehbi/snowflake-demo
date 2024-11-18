#!/bin/bash

# Define source path and stage path
source_path="Place path to files here"
stage_path="@PRODUCT_DATA"

# Ensure the stage path ends with a slash
stage_path="${stage_path%/}/"

# Loop through each file present in the source path
for filepath in "$source_path"/*; do
    # Get the base name of the file
    filename=$(basename "$filepath")

    # Construct the target path in the stage
    target_path="$stage_path"

    # Execute the PUT command to load the files into Snowflake
    echo "PUT file://$filepath $target_path;"
    snowsql -q "PUT file://$filepath $target_path;"
done