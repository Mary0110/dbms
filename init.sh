#!/bin/bash
docker run -d -e ORACLE_PWD=password -p 1521:1521 --name oracle-db container-registry.oracle.com/database/express:21.3.0-xe