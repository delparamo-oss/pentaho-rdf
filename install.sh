#!/bin/bash

mvn clean package -DskipTests

rm -r ../../../../update/data-integration/plugins/steps/*

cp target/*.zip ../../../../update/data-integration/plugins/steps/

cd ../../../../update/data-integration/plugins/steps/

unzip *.zip

cd "/home/cedia/cepal/newPlugns/new plugins/pdi-sdk-plugins/csv2rdf-step-plugin"

