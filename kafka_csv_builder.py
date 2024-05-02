import json
import csv
import pandas as pd
import os

size = 13
byte = 100

def parse_logs_and_export_to_csv(logfile_path, output_csv_path):
    # Open the input log file and create the output CSV file
    with open(logfile_path, 'r') as log_file, open(output_csv_path, 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        # Write the header with the new columns
        csv_writer.writerow([
            'message_size', 'throughput', 'strategy',
            'local_network_size', 'foreign_network_size'
        ])
        
        current_file_content = ""
        
        # Process each line in the log file
        for line in log_file:
            if line.startswith('==>'):
                if current_file_content:
                    # Process the content when a new file block starts or at end of file
                    process_log_entry(current_file_content, csv_writer)
                    current_file_content = ""  # Reset for the next block
            else:
                # Accumulate lines of the current log block
                current_file_content += line.strip()
                
        # Don't forget to process the last block
        if current_file_content:
            process_log_entry(current_file_content, csv_writer)

def process_log_entry(log_entry, csv_writer):
    global size, byte

    # Load the json content within the "content" field
    try:
        log_json = json.loads(log_entry)
        content = json.loads(log_json['content'])
        # Extract the Overall_Throughput_MPS and write to the CSV with the constant values
        csv_writer.writerow([
            byte, content['Overall_Throughput_MPS'], 'Kafka', size, size
        ])
    except json.JSONDecodeError:
        print("Error decoding JSON from log entry")

def concatenate_csv_files(directory_path, output_file_path):
    # List to store dataframes
    df_list = []
    
    # Iterate over all files in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        # Check if it's a file and ends with .csv
        if os.path.isfile(file_path) and filename.endswith('.csv'):
            # Read the CSV file and append to the list
            df = pd.read_csv(file_path)
            df_list.append(df)
    
    # Concatenate all dataframes in the list
    if df_list:
        concatenated_df = pd.concat(df_list, ignore_index=True)
        concatenated_df.sort_values(by=['local_network_size', 'message_size'], ascending=[True, True], inplace=True)
        # Write the concatenated dataframe to a new CSV file
        concatenated_df.to_csv(output_file_path, index=False)
    else:
        print("No CSV files found in the directory.")

# generate .csv given input.txt
# parse_logs_and_export_to_csv('input.txt', f"size-{size}-{byte}-byte.csv")

# concatenate all .csv files with same columns into one big .csv file
concatenate_csv_files('./results', 'output.csv')