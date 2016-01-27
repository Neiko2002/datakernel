#!/bin/bash

jOOQ=~/Downloads/jOOQ-3.7.2/jOOQ-lib
MYSQL=~/Downloads/mysql-connector-java-5.1.38/mysql-connector-java-5.1.38-bin.jar

set -x
java -cp $jOOQ/jooq-3.7.2.jar:$jOOQ/jooq-meta-3.7.2.jar:$jOOQ/jooq-codegen-3.7.2.jar:$MYSQL:. org.jooq.util.GenerationTool jooq-db-generator-config.xml

