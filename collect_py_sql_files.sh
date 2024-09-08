#!/bin/bash

# Directory to search recursively
directory="."

# Output file
output_file="all_files_content.txt"

# Array of subfolders to exclude (absolute or relative paths from $directory)
exclude_folders=(
    "./.venv"
    "./.vscode"
    "./.git"
    "./.pytest_cache"
    "./__pycache__"
    "./.idea"
    "./.DS_Store"
    # Add more folders as needed
)

# Array of file patterns to exclude
exclude_files=(
    "*.pyc"
    "*.pyo"
    "*.log"
    "*.png"
    # Add more patterns as needed
)

# Function to check if a folder should be excluded
should_exclude() {
    local folder_path="$1"
    for excluded_folder in "${exclude_folders[@]}"; do
        if [[ "$folder_path" == *"$excluded_folder"* ]]; then
            return 0
        fi
    done
    return 1
}

# Default to searching Python and SQL files
file_pattern="*.py"
include_sql=false

# Parse command-line options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -p|--python-only)
        file_pattern="*.py"
        include_sql=false
        shift # past argument
        ;;
        -s|--include-sql)
        file_pattern="*.py *.sql"
        include_sql=true
        shift # past argument
        ;;
        *)
        # unknown option
        echo "Usage: $0 [-p|--python-only] [-s|--include-sql]"
        exit 1
        ;;
    esac
done

# Find all files recursively and write their contents to output file
find "$directory" -type f \( -name "$file_pattern" \) | while IFS= read -r file; do
    # Check if the file's directory or any parent directory should be excluded
    dir_path=$(dirname "$file")
    exclude=false
    for folder in "${exclude_folders[@]}"; do
        if [[ "$dir_path" == *"$folder"* ]]; then
            exclude=true
            break
        fi
    done
    # Check if the file matches any exclude patterns
    for pattern in "${exclude_files[@]}"; do
        if [[ "$file" == $pattern ]]; then
            exclude=true
            break
        fi
    done
    if $exclude; then
        continue
    fi
    echo "==== $file ====" >> "$output_file"
    cat "$file" >> "$output_file"
    echo "" >> "$output_file"  # Add a blank line between files for clarity
done

echo "Content of all files (excluding specified folders and patterns) written to $output_file"
