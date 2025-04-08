import os
import requests
import pandas as pd
from sqlalchemy import create_engine, String, DateTime

# Configuration
API_URL = "https://cloud.soda.io/api/v1/checks?size=100"  # Taille par page
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
if not all(os.getenv(k) for k in ["SODA_CLOUD_API_KEY", "SODA_CLOUD_API_SECRET"]):
    print("ERREUR: Clés API manquantes dans les variables d'environnement")
    exit(1)

# Récupération des données avec pagination
all_checks = []
page = 0

print("Début de la récupération des données Soda Cloud...")

while True:
    try:
        # Requête paginée
        response = requests.get(
            f"{API_URL}&page={page}",
            auth=(os.getenv("SODA_CLOUD_API_KEY"), os.getenv("SODA_CLOUD_API_SECRET")),
            headers={"Accept": "application/json"},
            timeout=30
        )
        
        print(f"\nPage {page} - Statut HTTP: {response.status_code}")
        
        if response.status_code != 200:
            print(f"ERREUR API: {response.text[:500]}")
            break
            
        data = response.json()
        
        # Validation de la structure de la réponse
        if not isinstance(data, dict) or "content" not in data:
            print("ERREUR: Structure API inattendue")
            print(f"Clés disponibles: {data.keys()}")
            break
            
        checks = data.get("content", [])
        all_checks.extend(checks)
        
        # Pagination
        total_pages = data.get("totalPages", 1)
        print(f"Page {page+1}/{total_pages} traitée - {len(checks)} éléments")
        
        if page >= total_pages - 1:
            break
            
        page += 1
        
    except requests.exceptions.RequestException as e:
        print(f"ERREUR Réseau/API: {str(e)}")
        break
    except Exception as e:
        print(f"ERREUR Inattendue: {str(e)}")
        break

# Transformation des données
print("\nDébut de la transformation des données...")

try:
    df = pd.DataFrame(all_checks)
    
    # Gestion des colonnes manquantes
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        print(f"AVERTISSEMENT: Colonnes manquantes - {missing_columns}")
        df = df.reindex(columns=REQUIRED_COLUMNS, fill_value=None)
    
    df = df[REQUIRED_COLUMNS]
    
    # Conversion des dates avec gestion des erreurs
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True)
    df['lastCheckRunTime'] = pd.to_datetime(df['lastCheckRunTime'], errors='coerce', utc=True)
    
    # Nettoyage des données
    df['evaluationStatus'] = df['evaluationStatus'].astype(str).str[:50]
    df['column'] = df['column'].astype(str).str[:255]
    
    print(f"Données transformées - {len(df)} lignes")
    
except Exception as e:
    print(f"ERREUR Transformation: {str(e)}")
    exit(1)

# Écriture dans PostgreSQL
print("\nDébut de l'écriture dans PostgreSQL...")

try:
    # Debug de la connexion
    conn_string = f"postgresql+psycopg2://{os.getenv('POSTGRES_USERNAME')}:****@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
    print(f"Connexion à : {conn_string.replace('****', os.getenv('POSTGRES_PASSWORD', 'MOT_DE_PASSE_MASQUÉ'))}")

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USERNAME')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}?sslmode=require",
        connect_args={
            "connect_timeout": 5,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10
        }
    )
    
    with engine.connect() as conn:
        # Vérification de la connexion
        if not conn.connection:
            print("ERREUR: Échec de la connexion à PostgreSQL")
            exit(1)
            
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
        
        print("Vérification de l'écriture...")
        result = conn.execute("SELECT COUNT(*) FROM soda_checks").scalar()
        print(f"SUCCÈS: {result} lignes écrites dans soda_checks")

except Exception as e:
    print(f"ERREUR PostgreSQL: {str(e)}")
    exit(1)
