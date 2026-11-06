import os
import requests
import json
import sys

POLARIS_URL = os.getenv("POLARIS_EXTERNAL_URI", os.getenv("POLARIS_URL", "http://localhost:8181")).rstrip("/").replace("/api/catalog", "")
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "ce13f9967b812cf5")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "5e60f1964fa0a76423e4ae35f598b572")
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

    # Create namespace ecommerce
    ns_url = f"{POLARIS_URL}/api/catalog/v1/polaris/namespaces"
    r_ns = requests.post(ns_url, headers=headers, json={"namespace": ["ecommerce"]})
    if r_ns.status_code in (200, 201):
        print("Namespace 'ecommerce' created successfully!")
    elif r_ns.status_code == 409:
        print("Namespace 'ecommerce' already exists.")
    else:
        print(f"Namespace status: {r_ns.status_code} {r_ns.text}")

    # Register Iceberg tables
    reg_url = f"{POLARIS_URL}/api/catalog/v1/polaris/namespaces/ecommerce/register"
    tables = [
        ("orders", f"s3://{S3_BUCKET}/warehouse/ecommerce/orders/metadata/00005-1e5875dd-5616-4035-9031-dfa3ad567927.metadata.json"),
        ("order_payments", f"s3://{S3_BUCKET}/warehouse/ecommerce/order_payments/metadata/00005-c981b05e-cca7-4be5-b6c8-ece64a162dc4.metadata.json"),
        ("order_items", f"s3://{S3_BUCKET}/warehouse/ecommerce/order_items/metadata/00004-5408dfed-154f-488b-a57f-e7b5282f3f0d.metadata.json")
    ]
    for tbl_name, meta_loc in tables:
        r_tbl = requests.post(reg_url, headers=headers, json={"name": tbl_name, "metadata-location": meta_loc})
        if r_tbl.status_code in (200, 201):
            print(f"Table '{tbl_name}' registered successfully!")
        elif r_tbl.status_code == 409:
            print(f"Table '{tbl_name}' already registered.")
        else:
            print(f"Table '{tbl_name}' registration: {r_tbl.status_code} {r_tbl.text}")

if __name__ == "__main__":
    create_catalog()
