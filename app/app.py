import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import json

def fetch_github_data(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data

token = "Ton_token"
headers = {"Authorization": f"Bearer {token}"}
url_issues = "https://api.github.com/repos/fastapi/fastapi/issues"


issues_data = fetch_github_data(url_issues, headers)

df = pd.DataFrame(issues_data)
table = pa.Table.from_pandas(df)
pq.write_table(table, "issues.parquet")

with open("issues.json", "w") as f:
    json.dump(issues_data, f, indent=4)
