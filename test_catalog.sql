CREATE CATALOG test_cat WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 's3a://lakehouse/warehouse',
  'property-version' = '1',
  'fs.s3a.endpoint' = 'http://minio:9000',
  'fs.s3a.path.style.access' = 'true',
  'fs.s3a.access.key' = 'minioadmin',
  'fs.s3a.secret.key' = 'minioadmin123',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin123'
);
