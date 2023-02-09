CREATE DATABASE dataregistry;
CREATE USER 'dataregistry'@'%' IDENTIFIED BY 'dataregistry';
GRANT ALL PRIVILEGES ON dataregistry.* TO 'dataregistry'@'%' WITH GRANT OPTION;
