CREATE DATABASE IF NOT EXISTS lego;

-- -- Ensure the dblink extension is available
-- CREATE EXTENSION IF NOT EXISTS dblink;
--
-- -- Create the database if it does not exist
-- DO
-- $do$
-- BEGIN
--    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'lego') THEN
--       PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE lego');
--    END IF;
-- END
-- $do$;