#!/bin/bash

if [[ $# != 1 ]] ; then
  echo "Usage: $0 <name-of-new-database>"
  exit 1
fi


if [[ `psql -l | grep -c postgis_template` != 1 ]] ; then
  echo "You must have a postgis_template database already."
  exit 1
fi

createdb --template=postgis_template "$1"

