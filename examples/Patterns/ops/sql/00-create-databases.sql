-- Execute with psql as a role allowed to create databases.
SELECT 'CREATE DATABASE orders' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='orders') \gexec
SELECT 'CREATE DATABASE payment' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='payment') \gexec
SELECT 'CREATE DATABASE inventory' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='inventory') \gexec
SELECT 'CREATE DATABASE shipping' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='shipping') \gexec
SELECT 'CREATE DATABASE saga' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='saga') \gexec
SELECT 'CREATE DATABASE integration_read' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='integration_read') \gexec
SELECT 'CREATE DATABASE retry_schedule' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='retry_schedule') \gexec
SELECT 'CREATE DATABASE streamstate' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='streamstate') \gexec
SELECT 'CREATE DATABASE analytics' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='analytics') \gexec
