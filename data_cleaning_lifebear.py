import pandas as pd
import numpy as np
import os

pd.set_option('future.no_silent_downcasting', True) #Bypasses a warning about Pandas downcasting in the future

file_path = "insert file path"

# Read and preview the dataset
lifebear_dataset = pd.read_csv(file_path, sep=';', low_memory=False)

def split_csv_by_size(file_path, output_dir, garbage_file, cleaned_file, chunk_size_mb=100, column_to_remove=None):
    # Get the file size in MB
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to megabytes

    # Estimate the number of rows per chunk
    sample_chunk = pd.read_csv(file_path, sep=';', nrows=100, parse_dates=['birthday_on'])  # Read a sample of 100 rows to estimate row size
    avg_row_size = sample_chunk.memory_usage(deep=True).sum() / 100  # Average row size in bytes
    rows_per_chunk = int((chunk_size_mb * 1024 * 1024) / avg_row_size)  # Calculate number of rows per 100MB chunk

    print(f"Splitting into chunks of ~{chunk_size_mb} MB, approx. {rows_per_chunk} rows per chunk.")

    chunk_count = 0
    garbage_data = []  # Duplicates and unnecessary info will be stored here

    # Ensure the cleaned data file is created/emptied before appending chunks
    with open(cleaned_file, 'w', encoding='utf-8-sig') as f:
        f.write('')  

    # Read the CSV in chunks based on the estimated row count per chunk
    for chunk in pd.read_csv(file_path, sep=';', parse_dates=['birthday_on'], chunksize=rows_per_chunk):
         
      if 'created_at' in chunk.columns:
            # Store the unnecessary column data for garbage collection
            garbage_col_data = chunk[['created_at']].copy()
            garbage_data.append(garbage_col_data)
            # Remove the column from the chunk
            chunk = chunk.drop(columns=['created_at'])
            
        # Handle missing values based on column types
      for col in chunk.columns:
          if chunk[col].dtype == 'object':  # String/object columns
                # Fill missing values with 'N/A' for object columns
              chunk[col] = chunk[col].fillna('N/A').str.strip()
          else:  # Numeric columns
                # Fill missing values with 0 for numeric columns (or np.nan if preferred)
              chunk[col] = chunk[col].fillna(np.nan)

        # Explicitly infer objects after filling missing values to avoid future downcasting behavior
      chunk = chunk.infer_objects(copy=False)

        # Remove duplicate entries based on specific columns (e.g., 'login_id' and 'mail_address')
      if 'login_id' in chunk.columns and 'mail_address' in chunk.columns:
          duplicates = chunk[chunk.duplicated(subset=['login_id', 'mail_address'], keep=False)]
          if not duplicates.empty:  # Only append if duplicates are found
              garbage_data.append(duplicates)
            # Drop duplicates but keep the first occurrence
          chunk = chunk.drop_duplicates(subset=['login_id', 'mail_address'], keep='first')

        # Standardize all string data to lowercase (assuming string data)
      chunk = chunk.apply(lambda col: col.map(lambda x: x.lower() if isinstance(x, str) else x))

        # Save each cleaned chunk to a new CSV file
      chunk_file = os.path.join(output_dir, f"chunk_{chunk_count}.csv")
      chunk.to_csv(chunk_file, index=False, encoding='utf-8-sig')

        # Append the cleaned chunk to the final cleaned data file
      chunk.to_csv(cleaned_file, mode='a', header=(chunk_count == 0), index=False, encoding='utf-8-sig')

      print(f"Saved {chunk_file} (chunk {chunk_count}) and appended to {cleaned_file}")
      chunk_count += 1

    # Save all duplicate rows (garbage) to a separate file if duplicates exist
    if garbage_data:
        garbage_df = pd.concat(garbage_data, ignore_index=True)
        garbage_df.to_csv(garbage_file, index=False, encoding='utf-8-sig')
        print(f"Saved duplicates to {garbage_file}")
    else:
        print("No duplicates found, garbage file not created.")

# Specify the directory where you want to save the smaller files
output_dir = "insert output dir"
garbage_file = os.path.join(output_dir, "garbage.csv")
cleaned_file = os.path.join(output_dir, "cleaned_data.csv")

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Call the function to split the CSV, clean the data, merge chunks and create garbage file
split_csv_by_size(file_path, output_dir, garbage_file, cleaned_file,column_to_remove='created_at')
