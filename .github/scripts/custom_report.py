import os
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime, text
import logging
import json
from sqlalchemy.dialects.postgresql import JSONB

logging.basicConfig(level=logging.INFO)

# Configuration
API_URL = "https://cloud.soda.io/api/v1/checks?size=100"
REQUIRED_COLUMNS = [
    "id",
    "name",
    "evaluationStatus",
    "lastCheckRunTime",
    "column",
    "definition",
    "cloudUrl",
    "createdAt",
    "datasets",
    "attribute"
]

# Vérification des variables d'environnement
if not all(os.getenv(k) for k in ["SODA_CLOUD_API_KEY", "SODA_CLOUD_API_SECRET"]):
    logging.error("Clés API manquantes dans les variables d'environnement")
    exit(1)

# Récupération des données
all_checks = []
page = 0

logging.info("Début de la récupération des données Soda Cloud...")

while True:
    try:
        response = requests.get(
            f"{API_URL}&page={page}",
            auth=(os.getenv("SODA_CLOUD_API_KEY"), os.getenv("SODA_CLOUD_API_SECRET")),
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        logging.info(f"Page {page} - Statut HTTP: {response.status_code}")
        
        if response.status_code != 200:
            logging.error(f"ERREUR API: {response.text[:500]}")
            break
            
        data = response.json()
        
        if not isinstance(data, dict) or "content" not in data:
            logging.error("Structure API inattendue")
            logging.error(f"Clés disponibles: {data.keys()}")
            break
            
        checks = data.get("content", [])
        all_checks.extend(checks)
        
        total_pages = data.get("totalPages", 1)
        logging.info(f"Page {page+1}/{total_pages} traitée - {len(checks)} éléments")
        
        if page >= total_pages - 1:
            break
            
        page += 1
        
    except Exception as e:
        logging.exception(f"ERREUR: {str(e)}")
        break

# Transformation
logging.info("Début de la transformation des données...")

try:
    df = pd.DataFrame(all_checks)
    
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        logging.warning(f"Colonnes manquantes - {missing_columns}")
        df = df.reindex(columns=REQUIRED_COLUMNS, fill_value=None)
    
    # Conversion JSON et extraction des champs
    df['datasets'] = df['datasets'].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    df['attribute'] = df['attribute'].fillna('{}').apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    
    # Extraction des champs des datasets
    df['dataset_id'] = df['datasets'].apply(
        lambda x: json.loads(x)[0]['id'] if x and json.loads(x) else None
    )
    df['dataset_name'] = df['datasets'].apply(
        lambda x: json.loads(x)[0]['name'] if x and json.loads(x) else None
    )
    df['dataset_url'] = df['datasets'].apply(
        lambda x: json.loads(x)[0]['cloudUrl'] if x and json.loads(x) else None
    )
    
    # Extraction des attributs
    df['attributes'] = df['attribute'].apply(lambda x: json.loads(x) if x else {})
    
    # Exemple d'extraction d'attributs spécifiques
    df['priority'] = df['attributes'].apply(lambda x: x.get('priority'))
    df['owner'] = df['attributes'].apply(lambda x: x.get('owner'))
    
    df = df[REQUIRED_COLUMNS + ['dataset_id', 'dataset_name', 'dataset_url', 'priority', 'owner']]
    
    # Conversion des dates
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    df['lastCheckRunTime'] = pd.to_datetime(df['lastCheckRunTime'], errors='coerce', utc=True)
    
    # Nettoyage
    df['evaluationStatus'] = df['evaluationStatus'].astype(str).str[:50]
    df['column'] = df['column'].astype(str).str[:255]
    
    logging.info(f"Données transformées - {len(df)} lignes")
    
except Exception as e:
    logging.exception(f"ERREUR Transformation: {str(e)}")
    exit(1)

# PostgreSQL
logging.info("Début de l'écriture dans PostgreSQL...")

try:
    conn_string = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:****@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
    logging.info(f"Connexion à : {conn_string.replace('****', os.getenv('POSTGRES_PASSWORD', 'MOT_DE_PASSE_MASQUÉ'))}")

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}?sslmode=require",
        connect_args={
            "connect_timeout": 5,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10
        }
    )
    
    with engine.connect() as conn:
        if not conn.connection:
            logging.error("Échec de la connexion à PostgreSQL")
            exit(1)
            
        df.to_sql(
            name="certif_soda_checks_detailled_results",
            con=conn,
            if_exists="replace",
            index=False,
            dtype={
                "id": String(255),
                "name": String(255),
                "evaluationStatus": String(50),
                "lastCheckRunTime": DateTime(timezone=True),
                "column": String(255),
                "definition": String(),
                "cloudUrl": String(255),
                "createdAt": DateTime(timezone=True),
                "datasets": JSONB,
                "attribute": JSONB,
                "dataset_id": String(255),
                "dataset_name": String(255),
                "dataset_url": String(255),
                "priority": String(50),
                "owner": String(255)
            }
        )
        
        result = conn.execute(text("SELECT COUNT(*) FROM certif_soda_checks_detailled_results")).scalar()
        logging.info(f"SUCCÈS: {result} lignes écrites")

except Exception as e:
    logging.exception(f"ERREUR PostgreSQL: {str(e)}")
    exit(1)
