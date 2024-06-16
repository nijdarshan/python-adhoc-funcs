from tqdm import tqdm
import pandas as pd
import json

def ndjson_to_csv(ndjson_path, csv_path):
    raw_data = []
    with open(ndjson_path, 'r', encoding='utf-8') as ndjson:
        for line in tqdm([ndjson]):
            json_line = json.loads(line.strip())
            raw_data.append(json_line)
    df = pd.DataFrame(raw_data)
    df.to_csv(csv_path)
