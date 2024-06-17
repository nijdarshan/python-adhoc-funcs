import pandas as pd
import os
import numpy as np
from datetime import datetime


def sample_csv(sample_size, num_files, save_dir, index_file, keep_columns, big_csv_file, random_state=None):
    # Ensure the save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Initialize the set of existing indexes
    existing_indexes = set()

    # Check if index file exists, if not, create it and add indexes from existing CSV files in the save_dir
    if not os.path.exists(index_file):
        with open(index_file, 'w') as f:
            for filename in os.listdir(save_dir):
                if filename.endswith(".csv"):
                    existing_df = pd.read_csv(os.path.join(save_dir, filename))
                    existing_indexes.update(existing_df.index)
                    for idx in existing_df.index:
                        f.write(f"{idx}\n")
    else:
        # Read the index file
        if os.path.getsize(index_file) > 0:
            index_df = pd.read_csv(index_file, header=None)
            existing_indexes = set(index_df[0].tolist())

    # Load the big CSV file
    df = pd.read_csv(big_csv_file)
    
    # Extract new samples
    np.random.seed(random_state)
    new_indexes = set()
    while len(new_indexes) < sample_size:
        sampled_df = df.sample(n=sample_size-len(new_indexes), random_state=random_state)
        new_indexes.update(sampled_df.index.difference(existing_indexes))

    # Add new indexes to the index file
    with open(index_file, 'a') as f:
        for idx in new_indexes:
            f.write(f"{idx}\n")

    # Filter the new sampled DataFrame
    new_sampled_df = df.loc[list(new_indexes)]

    # Keep only specified columns if any
    if keep_columns:
        new_sampled_df = new_sampled_df[keep_columns]

    # Split into the specified number of files
    split_dfs = np.array_split(new_sampled_df, num_files)
    
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for i, split_df in enumerate(split_dfs):
        split_df.to_csv(os.path.join(save_dir, f'sampled_part_{i+1}_{timestamp}.csv'), index=False)

# Example usage:
#sample_csv(10000, 2, 'Anxiety_submissions/anxiety_samples', 'Anxiety_submissions/anxiety_samples/index', ['selftext'], 'Anxiety_submissions/Anxiety_Submissions.csv')


  
