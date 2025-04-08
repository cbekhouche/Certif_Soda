import os
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime

# Configuration
API_URL = "https://cloud.soda.io/api/v1/checks?size=1000"  # Augmenter la taille de la pagination[6]
DYNAMIC_COLUMNS = {
    "id": "check_id",
    "dataset": "dataset_name",
    "checkName": "check_name",
    "createdAt": "created_at",
    "dimension": "dimension",
    "status": "status"
}

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

# Vérification de la structure
if not isinstance(raw_data, dict) or "items" not in raw_data:
    print(f"ERREUR: Réponse mal structurée. Clés disponibles : {raw_data.keys()}")
    exit(1)

# Transformation
try:
    df = pd.json_normalize(raw_data["items"])
    print(f"DEBUG - Colonnes avant renommage : {df.columns.tolist()}")
    
    # Renommage dynamique
    df = df.rename(columns=DYNAMIC_COLUMNS)
    df = df[[col for col in DYNAMIC_COLUMNS.values() if col in df.columns]]
    
    # Gestion des colonnes manquantes
    missing = [col for col in DYNAMIC_COLUMNS.values() if col not in df.columns]
    if missing:
        print(f"AVERTISSEMENT : Colonnes manquantes - {missing}")
        df = df.assign(**{col: None for col in missing})  # Crée les colonnes manquantes
    
    print(f"DEBUG - Données transformées :\n{df.head()}")
    
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
                "check_id": String(255),
                "dataset_name": String(150),
                "dimension": String(100),
                "status": String(50),
                "created_at": DateTime(timezone=True)
            }
        )
except Exception as e:
    print(f"ERREUR POSTGRESQL : {str(e)}")
    exit(1)

print("SUCCÈS : Données stockées dans PostgreSQL")
