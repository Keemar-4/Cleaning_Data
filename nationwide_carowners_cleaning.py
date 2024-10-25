import pandas as pd
import numpy as np
import os

pd.set_option('future.no_silent_downcasting', True) #This line prevents a warning about Pandas future downcasting

#Insert File path where dataset is stored here
file_path = "insert file path"

# Mapping of Chinese column names to English (can be further customized)
column_mapping = {
    '车架号': 'vehicle_frame_number',
    '姓名': 'name',
    '身份证': 'id_card_number',
    '性别': 'gender',
    '手机': 'mobile_phone',
    '邮箱': 'email',
    '省': 'province',
    '城市': 'city',
    '地址': 'address',
    '邮编': 'postal_code',
    '生日': 'birthday',
    '行业': 'industry',
    '月薪': 'monthly_salary',
    '婚姻': 'marital_status',
    '教育': 'education',
    'BRAND': 'brand',
    '车系': 'car_series',
    '车型': 'car_model',
    '配置': 'configuration',
    '颜色': 'colour',
    '发动机号': 'engine_number'
}

# Regular expression to validate email format
email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

def validate_emails(chunk):
    """Validate the emails in the chunk and return invalid emails."""
    if 'email' in chunk.columns:
        invalid_emails = chunk[~chunk['email'].apply(lambda x: bool(re.match(email_pattern, x)) if pd.notnull(x) else False)]
        return invalid_emails
    return pd.DataFrame()  # Return empty if email column does not exist

def split_csv_by_size(file_path, output_dir, garbage_file, duplicates_file, cleaned_file, chunk_size_mb=100):
    # Get the file size in MB
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to megabytes

    # Estimate the number of rows per chunk
    sample_chunk = pd.read_csv(file_path, sep=',', nrows=100, encoding='utf-8')  # Read a sample of 100 rows to estimate row size
    avg_row_size = sample_chunk.memory_usage(deep=True).sum() / 100  # Average row size in bytes
    rows_per_chunk = int((chunk_size_mb * 1024 * 1024) / avg_row_size)  # Calculate number of rows per 100MB chunk

    print(f"Splitting into chunks of ~{chunk_size_mb} MB, approx. {rows_per_chunk} rows per chunk.")

    chunk_count = 0
    garbage_data = []  # Collect unnecessary column data here
    duplicates_data = []  # Collect duplicate rows here
    invalid_email_count = 0  # Count of invalid emails

    # Ensure the cleaned data file is created/emptied before appending chunks
    with open(cleaned_file, 'w', encoding='utf-8-sig') as f:
        f.write('')  # Clear any content

    unwanted_columns = ['configuration', 'postal_code']  #Subset of unnecessary columns

    # Read the CSV in chunks based on the estimated row count per chunk
    for chunk in pd.read_csv(file_path, encoding='utf-8', parse_dates=['生日'], chunksize=rows_per_chunk, low_memory=False):

        for col in chunk.columns:
            if col in column_mapping:
                # Rename columns
                chunk.rename(columns={col: column_mapping[col]}, inplace=True)

        # Check if any unwanted columns exist in the chunk
        if any(col in chunk.columns for col in unwanted_columns):
            # Store the unnecessary column data for garbage collection
            garbage_col_data = chunk[unwanted_columns].copy()
            garbage_data.append(garbage_col_data)
            # Remove the columns from the chunk
            chunk = chunk.drop(columns=unwanted_columns)

        for col in chunk.columns:
            if chunk[col].dtype == 'object':  # String/object columns
                # Fill missing values with 'N/A' for object columns
                chunk[col] = chunk[col].fillna('N/A').str.strip()
            else:  # Numeric columns
                # Fill missing values with np.nan for numeric columns
                chunk[col] = chunk[col].fillna(np.nan)

        #Data Cleaning Steps

        # 1. Validate emails and save invalid emails to garbage
        invalid_emails = validate_emails(chunk)
        if not invalid_emails.empty:
            garbage_data.append(invalid_emails)
            invalid_email_count += len(invalid_emails)
            print(f"Found {len(invalid_emails)} invalid emails in chunk {chunk_count}")

        # 2. Remove duplicate entries, save them to 'duplicates' and keep unique rows
        subset_columns = ['name', 'id_card_number', 'engine_number']
        duplicates = chunk[chunk.duplicated(subset=subset_columns, keep=False)]
        if not duplicates.empty:  # Only append if duplicates are found
            duplicates_data.append(duplicates)
        # Drop duplicates but keep the first occurrence
        chunk = chunk.drop_duplicates(subset=subset_columns, keep='first')

        # 3. Standardize all data to lowercase (assuming string data)
        chunk = chunk.apply(lambda col: col.str.lower() if col.dtype == 'object' else col)

        # Save each cleaned chunk to a new CSV file
        chunk_file = os.path.join(output_dir, f"chunk_{chunk_count}.csv")
        chunk.to_csv(chunk_file, index=False, encoding='utf-8-sig')

        # 4. Append the cleaned chunk to the final cleaned data file
        chunk.to_csv(cleaned_file, mode='a', header=(chunk_count == 0), index=False, encoding='utf-8-sig')

        print(f"Saved {chunk_file} (chunk {chunk_count}) and appended to {cleaned_file}")
        chunk_count += 1

    # Save all unnecessary column data (garbage) to a separate file
    if garbage_data:
        garbage_df = pd.concat(garbage_data, ignore_index=True)
        garbage_df.to_csv(garbage_file, index=False, encoding='utf-8-sig')
        print(f"Saved unnecessary columns data to {garbage_file}")
    else:
        print("No unnecessary columns found, garbage file not created.")

    # Save all duplicate rows to a separate file
    if duplicates_data:
        duplicates_df = pd.concat(duplicates_data, ignore_index=True)
        duplicates_df.to_csv(duplicates_file, index=False, encoding='utf-8-sig')
        print(f"Saved duplicates to {duplicates_file}")

        # Count the number of duplicates
        print(f"Total number of duplicates saved to duplicates file: {len(duplicates_df)}")

    else:
        print("No duplicates found, duplicates file not created.")


    # Print the total number of invalid emails saved to garbage
    print(f"Total number of invalid emails saved to garbage: {invalid_email_count}")

    # Count the number of entries in the cleaned data
    cleaned_df = pd.read_csv(cleaned_file, encoding='utf-8-sig',  )
    print(f"Total number of entries in the cleaned data: {len(cleaned_df)}")


# Specify the directory where you want to save the smaller files
output_dir = "insert directory"
garbage_file = os.path.join(output_dir, "garbage.csv")
duplicates_file = os.path.join(output_dir, "duplicates.csv")
cleaned_file = os.path.join(output_dir, "cleaned_data.csv")

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Call the function to split the CSV, clean the data, and merge chunks
split_csv_by_size(file_path, output_dir, garbage_file, duplicates_file, cleaned_file)
