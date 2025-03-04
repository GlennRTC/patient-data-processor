import os
import sys
import pandas as pd
import numpy as np

def safe_convert_to_float(value):
    """
    Safely convert value to float, handling various input types
    """
    if pd.isna(value):
        return np.nan
    
    try:
        # Remove any thousand separators and replace comma with dot
        if isinstance(value, str):
            value = value.replace(',', '.').replace(' ', '')
        
        # Convert to float
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def consolidate_patient_data(input_file, output_dir, chunk_size=50000):
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the CSV file with flexible parsing
    df = pd.read_csv(input_file, 
                     low_memory=False,
                     dtype=str)  # Read all columns as strings initially
    
    # Columns that should be numeric
    numeric_columns = ['HbA1c', 'uAlb', 'Creatinina', 'LDL', 'Creatinuria']
    
    # Convert numeric columns
    for col in numeric_columns:
        df[col] = df[col].apply(safe_convert_to_float)
    
    # Convert CollectDate to datetime with error handling
    df['CollectDate'] = pd.to_datetime(df['CollectDate'], format='%d/%m/%Y', errors='coerce')
    
    # Remove rows with invalid dates if needed
    df = df.dropna(subset=['CollectDate'])
    
    # Group by Patient and CollectDate, aggregating columns
    def aggregate_columns(group):
        # Create a dictionary to store the consolidated row
        consolidated = {}
        
        # List of columns to consolidate
        columns_to_check = [
            'Contrato', 'Sucursal', 'IPS_Primaria', 'HbA1c', 'uAlb', 
            'Creatinina', 'LDL', 'Creatinuria', 'Electrocardiograma', 
            'FechaResultado'
        ]
        
        # Iterate through columns and aggregate
        for col in columns_to_check:
            # Get non-null values for the column
            valid_values = group[col].dropna().unique()
            
            # If there are valid values, take the first non-null value
            if len(valid_values) > 0:
                consolidated[col] = valid_values[0]
            else:
                consolidated[col] = np.nan
        
        # Add Patient and CollectDate
        consolidated['Patient'] = group['Patient'].iloc[0]
        consolidated['CollectDate'] = group['CollectDate'].iloc[0]
        
        return pd.Series(consolidated)
    
    # Debugging: Print column info before processing
    print("Column dtypes before processing:")
    print(df.dtypes)
    
    # Apply the consolidation with chunking for large files
    consolidated_results = []
    
    # Group and process in chunks
    for patient_date, group in df.groupby(['Patient', 'CollectDate']):
        consolidated_results.append(aggregate_columns(group))
    
    # Convert list to DataFrame
    consolidated_df = pd.DataFrame(consolidated_results)
    
    # Sort by Patient and CollectDate
    consolidated_df = consolidated_df.sort_values(['Patient', 'CollectDate'])
    
    # Convert CollectDate back to original format for output
    consolidated_df['CollectDate'] = consolidated_df['CollectDate'].dt.strftime('%d/%m/%Y')
    
    # Chunk and save files
    total_rows = len(consolidated_df)
    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    output_files = []
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_rows)
        
        # Create chunk filename
        chunk_filename = os.path.join(output_dir, f'Consolidated_Results_part_{i+1}.csv')
        
        # Save chunk
        chunk = consolidated_df.iloc[start_idx:end_idx]
        chunk.to_csv(chunk_filename, index=False)
        output_files.append(chunk_filename)
        
        print(f"Saved chunk {i+1}/{num_chunks}: {chunk_filename}")
    
    # Create a summary file with chunk information
    summary_file = os.path.join(output_dir, 'chunk_summary.txt')
    with open(summary_file, 'w') as f:
        f.write(f"Total rows: {total_rows}\n")
        f.write(f"Number of chunks: {num_chunks}\n")
        f.write("Chunk files:\n")
        for chunk_file in output_files:
            f.write(f"- {os.path.basename(chunk_file)}\n")
    
    print(f"\nProcessing complete. Total rows: {total_rows}")
    print(f"Chunks saved in directory: {output_dir}")
    print(f"Summary file created: {summary_file}")
    
    return consolidated_df

def main():
    # Ensure input file exists
    input_file = 'input.csv'
    output_dir = 'Consolidated_Results'
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found!")
        sys.exit(1)
    
    # Process the file
    result = consolidate_patient_data(input_file, output_dir)
    
    # Display first few rows (optional)
    print("\nFirst few rows:")
    print(result.head())
    
    # Debugging: Print column info after processing
    print("\nColumn dtypes after processing:")
    print(result.dtypes)

if __name__ == "__main__":
    main()