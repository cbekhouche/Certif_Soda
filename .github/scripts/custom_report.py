import os
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime

# Configuration
API_URL = "https://cloud.soda.io/api/v1/checks"
REQUIRED_COLUMNS = ["check_id", "dataset_name", "dimension", "check_name", "status", "created_at"]

# Vérification des secrets
if not all(os.getenv(k) for k in ["SODA_CLOUD_API_KEY", "SODA_CLOUD_API_SECRET"]):
    print("ERREUR: Clés API manquantes")
    exit(1)

# Récupération des données avec pagination
all_items = []
page = 0
while True:
    try:
        response = requests.get(
            f"{API_URL}?page={page}&size=100",
            auth=(os.getenv("SODA_CLOUD_API_KEY"), os.getenv("SODA_CLOUD_API_SECRET")),
            headers={"Accept": "application/json"},
            timeout=15
        )
        
        if response.status_code != 200:
            print(f"ERREUR API: {response.status_code} - {response.text[:200]}")
            exit(1)
            
        data = response.json()
        all_items.extend(data.get("items", []))
        
        if page >= data.get("totalPages", 0):
            break
            
        page += 1
        
    except Exception as e:
        print(f"ERREUR: {str(e)}")
        exit(1)

# Transformation des données
try:
    df = pd.json_normalize(all_items)
    
    # Renommage des colonnes selon la documentation Soda[3][5]
    df = df.rename(columns={
        "id": "check_id",
        "dataset": "dataset_name", 
        "checkName": "check_name",
        "createdAt": "created_at"
    })
    
    if not all(col in df.columns for col in REQUIRED_COLUMNS):
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        print(f"COLONNES MANQUANTES: {missing}\nCOLONNES DISPONIBLES: {df.columns.tolist()}")
        exit(1)
        
    df = df[REQUIRED_COLUMNS]
    
    # Conversion des dates
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_convert(None)
    
except Exception as e:
    print(f"ERREUR TRANSFORMATION: {str(e)}")
    exit(1)

# Écriture dans PostgreSQL
try:
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USERNAME')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB')}"
    )
    
    with engine.connect() as conn:
        df.to_sql(
            name="soda_checks",
            con=conn,
            if_exists="replace",
            index=False,
            dtype={
                "check_id": String(255),
                "dataset_name": String(150),
                "dimension": String(100),
                "status": String(20),
                "created_at": DateTime()
            }
        )
        
except Exception as e:
    print(f"ERREUR POSTGRESQL: {str(e)}")
    exit(1)

print("SUCCÈS: Données stockées dans soda_checks")
