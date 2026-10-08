-- Password for the JDBC catalog role. Must match
-- POSTGRES_PASSWORD in docker-compose.yml and
-- spark.sql.catalog.my_catalog.jdbc.password in spark/spark-defaults.conf.
ALTER ROLE icebergcat PASSWORD 'iceberg';
