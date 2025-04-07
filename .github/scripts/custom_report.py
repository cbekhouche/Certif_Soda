import os
import requests
import pandas as pd
from sqlalchemy import create_engine

# 1. Configuration
API_URL = "https://cloud.soda.io/api/v1/reporting/checks"
REQUIRED_COLUMNS = ["id", "dataset", "dimension", "checkName", "status", "createdAt"]

# 2. Authentification Soda Cloud
soda_api_key_id = os.getenv("SODA_CLOUD_API_KEY")
soda_api_key_secret = os.getenv("SODA_CLOUD_API_SECRET")

if not soda_api_key_id or not soda_api_key_secret:
    print("ERREUR : Secrets Soda Cloud manquants (SODA_CLOUD_API_KEY/SODA_CLOUD_API_SECRET)")
    exit(1)

# 3. Récupération des données
try:
    response = requests.get(
        API_URL,
        auth=(soda_api_key_id, soda_api_key_secret),  # Authentification Basic Auth
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    
    # Debugging (à conserver)
    print(f"Statut HTTP : {response.status_code}")
    print(f"Headers : {dict(response.headers)}")
    print(f"Body (extrait) : {response.text[:500]}")
    
    response.raise_for_status()
    raw_data = response.json()

except requests.exceptions.RequestException as e:
    print(f"Erreur API : {str(e)}")
    exit(1)

# 4. Transformation
try:
    df = pd.json_normalize(raw_data["items"])
    
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        print(f"Colonnes manquantes : {missing_cols}")
        exit(1)
        
    df = df[REQUIRED_COLUMNS]

except KeyError as e:
    print(f"Structure JSON inattendue : {str(e)}")
    exit(1)

# 5. Écriture en base
try:
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USERNAME')}:{os.getenv('POSTGRES_PASSWORD')}"
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
        "POSTGRES_USERNAME": "test",
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
