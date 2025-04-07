# custom_report.py
import os
import requests
import pandas as pd
import psycopg2
import sqlalchemy==2.0.25

# Récupération depuis l'API Soda
response = requests.get(
    "https://cloud.soda.io/api/v1/reporting/checks",
    headers={"Authorization": f"ApiKey {os.getenv('SODA_API_KEY')}"}
)
data = response.json()["items"]

# Transformation
df = pd.json_normalize(data)
df = df[["id", "dataset", "dimension", "checkName", "status"]]  # Champs pertinents

# Écriture en base
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

df.to_sql("soda_custom_reports", conn, if_exists="replace", index=False)  # Nécessite SQLAlchemy
