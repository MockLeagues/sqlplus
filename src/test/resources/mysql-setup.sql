CREATE SCHEMA `sqlplus` ;
CREATE USER 'sqlplus' IDENTIFIED BY 'sqlplus';
GRANT ALL PRIVILEGES ON sqlplus.* To 'sqlplus';