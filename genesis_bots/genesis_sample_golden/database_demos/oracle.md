To demo oracle, you need to have a local oracle database. 
To install on osx, you can use docker:

docker pull container-registry.oracle.com/database/express:latest

-- for apple silicon

brew install colima  
brew install qemu
colima start --arch x86_64 --memory 4 --disk 40 --vm-type=qemu
export DOCKER_HOST="unix://${HOME}/.colima/docker.sock"

docker run -d \
  --name oracle-xe \
  -p 1521:1521 \
  -p 5500:5500 \
  -e ORACLE_PWD=my_password123 \
  container-registry.oracle.com/database/express:latest

docker logs oracle-xe -f
-- wait for DATABASE IS READY TO USE

-- get into sqlplus
docker exec -it oracle-xe sqlplus system/my_password123@XEPDB1
-- test connection with demo user
docker exec -it oracle-xe sqlplus oracle_demo/demo_password@XEPDB1
-- Grant execute permission on DBMS_METADATA package to oracle_demo user
-- Note: This requires SYSDBA privileges. Run as SYS user:

CONNECT sys/my_password123@XEPDB1 AS SYSDBA;
GRANT EXECUTE ON sys.dbms_metadata TO oracle_demo;


login with system/my_password

-- run demo script

docker cp ./demo/database_demos/oracle_demo.sql oracle-xe:/opt/oracle/
docker exec -it oracle-xe sqlplus sys/my_password123@XEPDB1 as sysdba @/opt/oracle/oracle_demo.sql





-- create demo database with data:
-- databases: movies, tv shows 

pip install oracledb

tell bot: 
add a new database connection to oracle called my_oracle using this connection string oracle+oracledb://oracle_demo:demo_password@localhost:1521/?service_name=XEPDB1

