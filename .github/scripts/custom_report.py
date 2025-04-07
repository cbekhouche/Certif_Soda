import os
import requests
import pandas as pd
from sqlalchemy import create_engine

# 1. Configuration
API_URL = "https://cloud.soda.io/api/v1/reporting/checks"
REQUIRED_COLUMNS = ["id", "dataset", "dimension", "checkName", "status", "createdAt"]

# 2. Récupération des données
try:
    response = requests.get(
        API_URL,
        headers={"Authorization": f"ApiKey {os.getenv('SODA_CLOUD_API_KEY')}"},
        timeout=10
    )
    response.raise_for_status()  # Lève une exception pour les codes 4xx/5xx
    raw_data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Erreur API : {str(e)}")
    exit(1)

# 3. Transformation
try:
    df = pd.json_normalize(raw_data["items"])
    
    # Gestion des colonnes manquantes
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        print(f"Colonnes manquantes : {missing_cols}")
        exit(1)
        
    df = df[REQUIRED_COLUMNS]
except KeyError as e:
    print(f"Structure JSON inattendue : {str(e)}")
    exit(1)

# 4. Écriture en base
try:
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
    )
    
    with engine.connect() as conn:
        df.to_sql(
            name="soda_custom_reports",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi"  # Optimisation pour les gros volumes
        )
except Exception as e:
    print(f"Erreur base de données : {str(e)}")
    exit(1)

print("Rapport généré avec succès!")
exit(0)

# Test unitaire simplifié (à ajouter en fin de script)
if __name__ == "__main__":
    # Mock des variables d'environnement
    os.environ.update({
        "SODA_CLOUD_API_KEY": "test",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_DB": "test",
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test"
    })
    
    # Test avec mock requests
    from unittest.mock import patch
    import json

    with patch("requests.get") as mock_get:
        # Mock réponse API
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "items": [{"id": "1", "dataset": "test", "dimension": "none", 
                      "checkName": "test_check", "status": "pass", "createdAt": "2024-01-01"}]
        }
        
        # Exécution du script
        exec(open(__file__).read())
