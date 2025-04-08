import os
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime

# Configuration
# Correction dans le script
API_URL = "https://cloud.soda.io/api/v1/checks"  # <-- URL corrigée
REQUIRED_COLUMNS = ["id", "dataset", "dimension", "checkName", "status", "createdAt"]

# Vérification des secrets
if not all(os.getenv(k) for k in ["SODA_CLOUD_API_KEY", "SODA_CLOUD_API_SECRET"]):
    print("ERREUR : Secrets manquants (SODA_CLOUD_API_KEY/SODA_CLOUD_API_SECRET)[1]")
    exit(1)

# Récupération des données
try:
    response = requests.get(
        API_URL,
        auth=(os.getenv("SODA_CLOUD_API_KEY"), os.getenv("SODA_CLOUD_API_SECRET")),
        headers={"Content-Type": "application/json"},
        timeout=15
    )
    
    if response.status_code != 200:
        print(f"ERREUR API : {response.status_code} - {response.text[:500]}")
        exit(1)
        
    raw_data = response.json()
except Exception as e:
    print(f"ERREUR : {str(e)}")
    exit(1)

# Transformation
try:
    df = pd.json_normalize(raw_data.get("items", []))
    
    if not all(col in df.columns for col in REQUIRED_COLUMNS):
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        print(f"COLONNES MANQUANTES : {missing}")
        exit(1)
        
    df = df[REQUIRED_COLUMNS]
except Exception as e:
    print(f"ERREUR TRANSFORMATION : {str(e)}")
    exit(1)

# Écriture PostgreSQL
try:
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USERNAME')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB')}"
    )
    
    with engine.connect() as conn:
        df.to_sql(
            name="soda_reports",
            con=conn,
            if_exists="replace",
            index=False,
            dtype={
                "id": String(255),
                "createdAt": DateTime(),
                "status": String(50)
            }
        )
except Exception as e:
    print(f"ERREUR POSTGRESQL : {str(e)}")
    exit(1)

print("SUCCÈS : Rapport généré dans PostgreSQL")
