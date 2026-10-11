import os
import requests
import json
import sys

POLARIS_URL = os.getenv("POLARIS_EXTERNAL_URI", os.getenv("POLARIS_URL", "http://localhost:8181")).rstrip("/").replace("/api/catalog", "")
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "a03cca35bfce7cf8")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "b6d7a396db32af73b5c2c9d41e8fa7b1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def create_catalog():
    print(f"Connecting to Polaris at {POLARIS_URL}...")
    token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
    data = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "scope": "PRINCIPAL_ROLE:ALL"}
    
    res = requests.post(token_url, data=data)
    res.raise_for_status()
    token = res.json()["access_token"]
    
    catalog_url = f"{POLARIS_URL}/api/management/v1/catalogs"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    t_res = requests.get(f"{POLARIS_URL}/api/catalog/v1/polaris/namespaces/ecommerce/tables", headers=headers)
    if t_res.status_code == 200:
        for t in t_res.json().get("identifiers", []):
            t_name = t["name"]
            requests.delete(f"{POLARIS_URL}/api/catalog/v1/polaris/namespaces/ecommerce/tables/{t_name}", headers=headers)
    d1 = requests.delete(f"{POLARIS_URL}/api/catalog/v1/polaris/namespaces/ecommerce", headers=headers)
    d2 = requests.delete(f"{catalog_url}/polaris", headers=headers)
    print("Delete namespace status:", d1.status_code, d1.text)
    print("Delete catalog status:", d2.status_code, d2.text)
    
    catalog_payload = {
        "catalog": {
            "name": "polaris",
            "type": "INTERNAL",
            "properties": {
                "default-base-location": f"s3://{S3_BUCKET}/warehouse",
                "s3.endpoint": S3_ENDPOINT,
                "client.s3.endpoint": S3_ENDPOINT,
                "s3.path-style-access": "true",
                "s3.path.style.access": "true",
                "client.s3.path-style-access": "true",
                "s3.region": AWS_REGION,
                "s3.access-key-id": MINIO_USER,
                "s3.secret-access-key": MINIO_PASS,
                "s3.access.key.id": MINIO_USER,
                "s3.secret.access.key": MINIO_PASS,
                "fs.s3a.access.key": MINIO_USER,
                "fs.s3a.secret.key": MINIO_PASS,
                "s3.credential-vending.enabled": "false"
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [f"s3a://{S3_BUCKET}/warehouse", f"s3://{S3_BUCKET}/warehouse"],
                "region": AWS_REGION,
                "endpoint": S3_ENDPOINT,
                "pathStyleAccess": True
            }
        }
    }
    res2 = requests.post(catalog_url, headers=headers, json=catalog_payload)
    if res2.status_code in (201, 200):
        print("Catalog 'polaris' created successfully!")
    elif res2.status_code == 409:
        print("Catalog 'polaris' already exists.")
    else:
        print(f"Failed to create catalog: {res2.text}")
        sys.exit(1)

if __name__ == "__main__":
    create_catalog()
