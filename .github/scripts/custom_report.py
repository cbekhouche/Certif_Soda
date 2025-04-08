import os
import sys
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime, text
from datetime import datetime, timezone
import re  # Import pour les regex

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

    # Extraction du dataset
    def extract_dataset(definition):
        match = re.search(r"checks for ([\w_]+)", definition)
        return match.group(1) if match else None

    df['dataset'] = df['definition'].apply(extract_dataset)

    # Nettoyage des colonnes
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    df['lastCheckRunTime'] = pd.to_datetime(df['lastCheckRunTime'], errors='coerce', utc=True)
    df['evaluationStatus'] = df['evaluationStatus'].astype(str).str[:50]
    df['column'] = df['column'].astype(str).str[:255]
    df['definition'] = df['definition'].astype(str).str[:1000]
    df['name'] = df['name'].astype(str).str[:255]
    df['dataset'] = df['dataset'].astype(str).str[:255]

    # Vérification et suppression des doublons
    initial_count = len(df)
    logger.info(f"Données initiales : {initial_count} lignes")
    df = df.drop_duplicates(subset=['id'])
    final_count = len(df)
    logger.info(f"Données transformées : {final_count} lignes ({initial_count - final_count} doublons supprimés)")

except Exception as e:
    logger.error(f"Erreur de transformation : {str(e)}")
    sys.exit(1)

# Connexion PostgreSQL et création de table
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
        # Suppression et recréation de la table
        conn.execute(text("DROP TABLE IF EXISTS soda_checks CASCADE"))
        
        create_table_query = """
        CREATE TABLE soda_checks (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            evaluationStatus VARCHAR(50),
            lastCheckRunTime TIMESTAMP WITH TIME ZONE,
            "column" VARCHAR(255),
            definition TEXT,
            cloudUrl VARCHAR(255),
            createdAt TIMESTAMP WITH TIME ZONE,
            dataset VARCHAR(255)  -- Ajout de la colonne dataset
        );
        """
        
        conn.execute(text(create_table_query))
        logger.info("Table soda_checks recréée avec succès")

        # Écriture des données
        chunksize = 50
        total_written = 0
        
        for i in range(0, len(df), chunksize):
            chunk = df[i:i + chunksize]
            
            try:
                chunk.to_sql(
                    name="soda_checks",
                    con=conn,
                    if_exists="append",
                    index=False,
                    dtype={
                        "id": String(255),
                        "name": String(255),
                        "evaluationStatus": String(50),
                        "lastCheckRunTime": DateTime(timezone=True),
                        "column": String(255),
                        "definition": String(1000),
                        "cloudUrl": String(255),
                        "createdAt": DateTime(timezone=True),
                        "dataset": String(255)  # Ajout de la colonne dataset
                    }
                )
                total_written += len(chunk)
                logger.info(f"Chunk {i//chunksize + 1} écrit : {len(chunk)} lignes")
            
            except Exception as chunk_error:
                logger.error(f"Erreur sur le chunk {i//chunksize + 1}: {str(chunk_error)}")
                logger.info("Tentative d'écriture ligne par ligne...")
                
                for _, row in chunk.iterrows():
                    try:
                        row_data = row.to_frame().T
                        row_data.to_sql(
                            name="soda_checks",
                            con=conn,
                            if_exists="append",
                            index=False,
                            dtype={
                                "id": String(255),
                                "name": String(255),
                                "evaluationStatus": String(50),
                                "lastCheckRunTime": DateTime(timezone=True),
                                "column": String(255),
                                "definition": String(1000),
                                "cloudUrl": String(255),
                                "createdAt": DateTime(timezone=True),
                                "dataset": String(255)  # Ajout de la colonne dataset
                            }
                        )
                        total_written += 1
                    except Exception as row_error:
                        logger.error(f"Échec sur l'ID {row['id']} : {str(row_error)}")
                        logger.info("Sauvegarde de la ligne problématique...")
                        row.to_frame().T.to_csv("erreurs_soda_checks.csv", mode='a', header=not os.path.exists("erreurs_soda_checks.csv"))
                
        # Vérification finale
        final_count = conn.execute(text("SELECT COUNT(*) FROM soda_checks")).scalar()
        logger.info(f"Vérification finale : {final_count} lignes")
        
        # Vérification des statistiques
        stats = conn.execute(text("""
            SELECT n_live_tup, n_dead_tup 
            FROM pg_stat_user_tables 
            WHERE relname = 'soda_checks'
        """)).fetchone()
        
        if stats:
            logger.info(f"Statistiques PostgreSQL - Lignes actives: {stats[0]}, Lignes mortes: {stats[1]}")
        
        # Ajout de la génération de rapports
        logger.info("Génération des rapports...")

        # Groupement par dataset
        dataset_report = pd.pivot_table(df, index="dataset", values="id", aggfunc="count", fill_value=0)
        logger.info("\nRapport par dataset:\n" + dataset_report.to_string())

        # Groupement par dimension
        dimension_report = pd.pivot_table(df, index="column", values="id", aggfunc="count", fill_value=0)
        logger.info("\nRapport par dimension:\n" + dimension_report.to_string())

        # Groupement par checks
        checks_report = pd.pivot_table(df, index="name", values="id", aggfunc="count", fill_value=0)
        logger.info("\nRapport par checks:\n" + checks_report.to_string())

except Exception as e:
    logger.error(f"ERREUR PostgreSQL : {str(e)}")
    sys.exit(1)

logger.info("Script exécuté avec succès")
