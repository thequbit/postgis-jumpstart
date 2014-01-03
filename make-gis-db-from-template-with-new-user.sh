#!/bin/bash

if [[ $# != 2 ]] ; then
  echo "Usage: $0 <name-of-new-database> <name-of-new-user>"
  exit 1
fi

if [[ `psql -l | grep -c postgis_template` != 1 ]] ; then
  echo "You must have a postgis_template database already."
  exit 1
fi

DB=$1
USER=$2

createuser --superuser ${USER}
createdb --owner=${USER} --template=postgis_template ${DB}
psql --dbname=${DB} --username=${USER} --no-password -c "ALTER ROLE ${USER} SET search_path TO '\$user',public,tiger;"

