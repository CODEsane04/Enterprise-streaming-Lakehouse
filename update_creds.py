import re

client_id = "a03cca35bfce7cf8"
client_secret = "b6d7a396db32af73b5c2c9d41e8fa7b1"
credential_str = f"{client_id}:{client_secret}"

# 0. Update .env
with open(".env", "r") as f:
    content = f.read()
content = re.sub(r'POLARIS_CLIENT_ID=.*', f'POLARIS_CLIENT_ID={client_id}', content)
content = re.sub(r'POLARIS_CLIENT_SECRET=.*', f'POLARIS_CLIENT_SECRET={client_secret}', content)
with open(".env", "w") as f:
    f.write(content)

# 1. Update scripts/bootstrap_polaris.py
with open("scripts/bootstrap_polaris.py", "r") as f:
    content = f.read()
content = re.sub(r'CLIENT_ID = os\.getenv\("POLARIS_CLIENT_ID", ".*?"\)', f'CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "{client_id}")', content)
content = re.sub(r'CLIENT_SECRET = os\.getenv\("POLARIS_CLIENT_SECRET", ".*?"\)', f'CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "{client_secret}")', content)
with open("scripts/bootstrap_polaris.py", "w") as f:
    f.write(content)

# 2. Update trino/etc/catalog/iceberg.properties
with open("trino/etc/catalog/iceberg.properties", "r") as f:
    content = f.read()
content = re.sub(r'iceberg\.rest-catalog\.oauth2\.credential=.*', f'iceberg.rest-catalog.oauth2.credential={credential_str}', content)
with open("trino/etc/catalog/iceberg.properties", "w") as f:
    f.write(content)

# 3. Update flink/sql/01-catalog.sql
with open("flink/sql/01-catalog.sql", "r") as f:
    content = f.read()
content = re.sub(r"'credential' = '.*?'", f"'credential' = '{credential_str}'", content)
with open("flink/sql/01-catalog.sql", "w") as f:
    f.write(content)

# 4. Update flink/sql/run_all.sql
with open("flink/sql/run_all.sql", "r") as f:
    content = f.read()
content = re.sub(r"'credential' = '.*?'", f"'credential' = '{credential_str}'", content)
with open("flink/sql/run_all.sql", "w") as f:
    f.write(content)

print("Updated credentials in all config files and .env.")
