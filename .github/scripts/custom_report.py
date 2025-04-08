import os
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime

# Configuration
API_URL = "https://cloud.soda.io/api/v1/checks?size=100"  # Augmenter la taille
REQUIRED_COLUMNS = [
    "id",
    "name",
    "evaluationStatus",
    "lastCheckRunTime",
    "column",
    "definition",
    "cloudUrl",
    "createdAt"
]

# Vérification des secrets
if not all(os.getenv(k) for k in ["SODA_CLOUD_API_KEY", "SODA_CLOUD_API_SECRET"]):
    print("ERREUR: Clés API manquantes")
    exit(1)

# Récupération des données
try:
    response = requests.get(
        API_URL,
        auth=(os.getenv("SODA_CLOUD_API_KEY"), os.getenv("SODA_CLOUD_API_SECRET")),
        headers={"Accept": "application/json"},
        timeout=30
    )
    
    print(f"DEBUG - Statut HTTP : {response.status_code}")
    print(f"DEBUG - En-têtes : {response.headers}")
    
    if response.status_code != 200:
        print(f"ERREUR API : {response.text[:500]}")
        exit(1)
        
    raw_data = response.json()
    print(f"DEBUG - Réponse API : {raw_data.keys()}")  # Vérification des clés principales
    
except Exception as e:
    print(f"ERREUR API : {str(e)}")
    exit(1)

# Extraction des données
try:
    checks = raw_data.get("content", [])
    df = pd.DataFrame(checks)

    # Sélection des colonnes
    if not all(col in df.columns for col in REQUIRED_COLUMNS):
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        print(f"ERREUR : Colonnes manquantes - {missing}")
        exit(1)

    df = df[REQUIRED_COLUMNS]
    
    # Conversion des dates
    df['createdAt'] = pd.to_datetime(df['createdAt'])
    df['lastCheckRunTime'] = pd.to_datetime(df['lastCheckRunTime'])

except Exception as e:
    print(f"ERREUR Transformation : {str(e)}")
    exit(1)

# Écriture PostgreSQL
try:
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USERNAME')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
    )
    
    with engine.connect() as conn:
        df.to_sql(
            name="soda_checks",
            con=conn,
            if_exists="replace",
            index=False,
            dtype={
                "id": String(255),
                "name": String(255),
                "evaluationStatus": String(50),
                "lastCheckRunTime": DateTime(timezone=True),
                "column": String(255),
                "definition": String(1000),
                "cloudUrl": String(255),
                "createdAt": DateTime(timezone=True)
            }
        )

except Exception as e:
    print(f"ERREUR PostgreSQL : {str(e)}")
    exit(1)

print("SUCCÈS : Données stockées dans soda_checks")
