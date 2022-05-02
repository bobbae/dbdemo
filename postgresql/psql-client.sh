docker exec -it postgresql psql -h localhost -p 5432 -U pguser -W -d testdb

#psql -h localhost -p 5432 -U pguser -W -d testdb
# docker exec -it postgresql psql -h localhost -p 5432 -U pguser -W -d postgres
# psql -h localhost -p 5432 -U pguser -W -d postgres
# CREATE DATABASE dfstore1;
# \c dfstore1;
# CREATE TABLE IF NOT EXISTS schema ( tablename VARCHAR(128) PRIMARY KEY, columns VARCHAR(255) NOT NULL );
# \l
