#!/bin/bash

# Def des variables
source_base_path="/mnt/c/temp/PRODUCT_FILES"  # Replace with your base source path
stage_path="@product_data"                    # Replace with your Snowflake stage

# chemin de la Stage se termine par un slash
stage_path="${stage_path%/}/"

# Loop sur chaque sous dossier dans le chemin 
for subfolder in "$source_base_path"/*; do
    # Vérifier le nom sous le format : YYYY_MM_DD
    if [[ "$(basename "$subfolder")" =~ ^[0-9]{4}_[0-9]{2}_[0-9]{2}$ ]]; then
        echo "Processing subfolder: $subfolder"

        # Loop sur chaque .csv file dans les sous-dossiers valides
        for filepath in "$subfolder"/*.csv; do
            # Check si le(s) fichier existe (au cas où le dossier est vide)
            if [ -e "$filepath" ]; then
                # le nom du fichier
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
