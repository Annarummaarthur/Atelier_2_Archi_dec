from fastapi import FastAPI
import httpx
import pyarrow.parquet as pq
import pyarrow as pa

app = FastAPI()

GITHUB_TOKEN = "votre_token_personnel"
HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
REPO = "utilisateur/nom_du_depot"

@app.get("/commits")
async def get_commits():
    # Récupération des données depuis l'API GitHub
    url = f"https://api.github.com/repos/{REPO}/commits"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=HEADERS)
        commits = response.json()

    # Transformation en format Parquet
    data = [{"sha": c["sha"], "author": c["commit"]["author"]["name"], "date": c["commit"]["author"]["date"]} for c in commits]
    table = pa.Table.from_pylist(data)
    pq.write_table(table, "data/commits.parquet")

    return {"message": "Commits collected and saved to data/commits.parquet"}
