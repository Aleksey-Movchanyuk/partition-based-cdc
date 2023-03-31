-- Create a new user
CREATE USER metadata_user WITH PASSWORD 'metadata_user';

-- Create a new database
CREATE DATABASE metadata_db;

-- Grant all privileges on the new database to the new user
GRANT ALL PRIVILEGES ON DATABASE metadata_db TO metadata_user;
