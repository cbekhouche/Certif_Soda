import os
import sys
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime, text, inspect

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

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
    "createdAt"
]

# Vérification des variables d'environnement
logger.info("Vérification des variables d'environnement...")
required_env_vars = [
    "SODA_CLOUD_API_KEY",
    "SODA_CLOUD_API_SECRET",
    "POSTGRES_HOST",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB"
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Variables manquantes : {missing_vars}")
    sys.exit(1)

# Récupération des données avec pagination
logger.info("Début de la collecte des données Soda Cloud...")
all_checks = []
page = 0

while True:
    try:
        response = requests.get(
            f"{API_URL}&page={page}",
            auth=(os.getenv("SODA_CLOUD_API_KEY"), os.getenv("SODA_CLOUD_API_SECRET")),
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        logger.info(f"Page {page} - Statut HTTP: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Erreur API : {response.text[:200]}...")
            break
            
        data = response.json()
        
        if not isinstance(data, dict) or "content" not in data:
            logger.error("Format de réponse API inattendu")
            logger.debug(f"Clés reçues : {data.keys()}")
            break
            
        checks = data.get("content", [])
        all_checks.extend(checks)
        
        total_pages = data.get("totalPages", 1)
        logger.info(f"Page {page+1}/{total_pages} traitée - {len(checks)} éléments")
        
        if page >= total_pages - 1:
            break
            
        page += 1
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur réseau : {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur inattendue : {str(e)}")
        sys.exit(1)

# Transformation des données
logger.info("Transformation des données...")

try:
    df = pd.DataFrame(all_checks)
    
    # Vérification des colonnes
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.warning(f"Colonnes manquantes : {missing_cols}")
        df = df.reindex(columns=REQUIRED_COLUMNS, fill_value=None)
    
    df = df[REQUIRED_COLUMNS]
    
    # Nettoyage des données
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    df['lastCheckRunTime'] = pd.to_datetime(df['lastCheckRunTime'], errors='coerce', utc=True)
    
    df['evaluationStatus'] = df['evaluationStatus'].astype(str).str[:50]
    df['column'] = df['column'].astype(str).str[:255]
    df['definition'] = df['definition'].astype(str).str[:1000]
    
    # Suppression des doublons
    initial_count = len(df)
    df = df.drop_duplicates(subset=['id'])
    final_count = len(df)
    
    logger.info(f"Données transformées : {final_count} lignes ({initial_count - final_count} doublons supprimés)")
    
except Exception as e:
    logger.error(f"Erreur de transformation : {str(e)}")
    sys.exit(1)

# Connexion PostgreSQL
logger.info("Connexion à PostgreSQL...")

try:
    conn_string = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
        "?sslmode=require"
        "&connect_timeout=5"
        "&keepalives=1"
        "&keepalives_idle=30"
    )
    
    engine = create_engine(conn_string, pool_pre_ping=True)
    
    with engine.connect() as conn:
        # Vérification de la connexion
        conn.execute(text("SELECT 1"))
        logger.info("Connexion réussie à PostgreSQL")
        
        # Vérification de la table existante
        inspector = inspect(engine)
        table_exists = inspector.has_table("soda_checks")
        
        if table_exists:
            old_count = conn.execute(text("SELECT COUNT(*) FROM soda_checks")).scalar()
            logger.info(f"La table existe déjà avec {old_count} lignes")
        else:
            logger.info("La table n'existe pas, elle sera créée")
            old_count = 0

        # Écriture des données
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
        
        # Vérification finale
        new_count = conn.execute(text("SELECT COUNT(*) FROM soda_checks")).scalar()
        
        if new_count != len(df):
            logger.error(f"Écart détecté : {len(df)} lignes attendues vs {new_count} écrites")
            
            # Comparaison des IDs
            db_ids = [r[0] for r in conn.execute(text("SELECT id FROM soda_checks")).fetchall()]
            df_ids = df['id'].tolist()
            
            missing_in_db = set(df_ids) - set(db_ids)
            extra_in_db = set(db_ids) - set(df_ids)
            
            logger.error(f"IDs manquants dans la base : {list(missing_in_db)[:5]}...")
            logger.error(f"IDs en trop dans la base : {list(extra_in_db)[:5]}...")
            
            sys.exit(1)
        
        logger.info(f"Écriture réussie : {new_count} lignes dans soda_checks")
        
except Exception as e:
    logger.error(f"Erreur PostgreSQL : {str(e)}")
    sys.exit(1)

logger.info("Script exécuté avec succès")
