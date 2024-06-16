from tqdm import tqdm
import pandas as pd
import json

def ndjson_to_csv(ndjson_path, csv_path):
    # get the total number of lines for progress bar
    total_lines = sum(1 for line in open(ndjson_path, 'r', encoding='utf-8'))
    
    raw_data = []
    with open(ndjson_path, 'r', encoding='utf-8') as ndjson:
        # read every line as json obj and append to list
        for line in tqdm(ndjson, total=total_lines, desc="Processing"):
            json_line = json.loads(line)
            raw_data.append(json_line)
    
    # convert list to CSV and save
    df = pd.DataFrame(raw_data)
    df.to_csv(csv_path, index=False)
