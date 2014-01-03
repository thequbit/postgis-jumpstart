#!/bin/bash

if [[ $# = 0 ]] ; then
  echo "Usage: $0 <state-abbreviation> [more state abbreviations]"
  exit 1
fi

set -e -u

WGET="wget --no-parent --relative --recursive --level=1 --accept=zip --mirror --reject=html --no-directories --no-verbose --no-clobber"
D="postgis_template"
P="psql -d ${D}"
TMP="/tmp/${D}_${$}"
mktemp -d $TMP
cd $TMP

svn co svn://svn.code.sf.net/p/pagc/code/branches/sew-refactor/postgresql address_standardizer
cd address_standardizer
make install
cd ..

psql -d postgres -c "CREATE DATABASE ${D};"
$P -c 'CREATE EXTENSION postgis;'
$P -c 'CREATE EXTENSION postgis_topology;'
$P -c 'CREATE EXTENSION fuzzystrmatch;'
$P -c 'CREATE EXTENSION postgis_tiger_geocoder;'
$P -c 'CREATE EXTENSION address_standardizer;'

$P -c "DROP TABLE IF EXISTS tiger_data.state_all;"
$P -c "CREATE TABLE tiger_data.state_all(CONSTRAINT pk_state_all PRIMARY KEY (statefp),CONSTRAINT uidx_state_all_stusps  UNIQUE (stusps), CONSTRAINT uidx_state_all_gid UNIQUE (gid)) INHERITS(state);"
$P -c "DROP TABLE IF EXISTS tiger_data.county_all;"
$P -c "CREATE TABLE tiger_data.county_all(CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)) INHERITS(county);"

$WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/STATE/
$WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/COUNTY/
find . -iname '*.zip' -exec unzip {} \;
find . -iname '*.zip' -delete

$P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
$P -c "CREATE SCHEMA tiger_staging;"
shp2pgsql -c -s 4269 -g the_geom -W "latin1" tl_2013_us_state.dbf tiger_staging.state | $P
shp2pgsql -c -s 4269 -g the_geom -W "latin1" tl_2013_us_county.dbf tiger_staging.county | $P

$P -c "SELECT loader_load_staged_data(lower('state'), lower('state_all'));"
$P -c "CREATE INDEX tiger_data_state_all_the_geom_gist ON tiger_data.state_all USING gist(the_geom);"

$P -c "ALTER TABLE tiger_staging.county RENAME geoid TO cntyidfp;"
$P -c "SELECT loader_load_staged_data(lower('county'), lower('county_all'));"
$P -c "CREATE INDEX tiger_data_county_the_geom_gist ON tiger_data.county_all USING gist(the_geom);"
$P -c "CREATE UNIQUE INDEX uidx_tiger_data_county_all_statefp_countyfp ON tiger_data.county_all USING btree(statefp,countyfp);"
$P -c "CREATE TABLE tiger_data.county_all_lookup ( CONSTRAINT pk_county_all_lookup PRIMARY KEY (st_code, co_code)) INHERITS (county_lookup);"
$P -c "INSERT INTO tiger_data.county_all_lookup(st_code, state, co_code, name) SELECT CAST(s.statefp as integer), s.abbrev, CAST(c.countyfp as integer), c.name FROM tiger_data.county_all As c INNER JOIN state_lookup As s ON s.statefp = c.statefp;"

$P -c "VACUUM ANALYZE tiger_data.state_all;"
$P -c "VACUUM ANALYZE tiger_data.county_all;"
$P -c "VACUUM ANALYZE tiger_data.county_all_lookup;"

create_state() {
  ABBREV=$1
  NUMBER=$($P -c "SELECT statefp FROM tiger.state_lookup WHERE abbrev = '${ABBREV}';" | grep '^ *[0-9][0-9]*$' | sed -e 's/[^0-9]//g')

  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/PLACE/tl_*_${NUMBER}_*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/COUSUB/tl_*_${NUMBER}_*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/TRACT/tl_*_${NUMBER}_*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/TABBLOCK/tl_*_${NUMBER}_*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/BG/tl_*_${NUMBER}_*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2010/ZCTA5/2010/*_${NUMBER}*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/FACES/*_${NUMBER}*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/FEATNAMES/*_${NUMBER}*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/EDGES/*_${NUMBER}*
  $WGET ftp://ftp2.census.gov/geo/tiger/TIGER2013/ADDR/*_${NUMBER}*
  find . -iname '*.zip' -exec unzip {} \;
  find . -iname '*.zip' -delete

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_place(CONSTRAINT pk_${ABBREV}_place PRIMARY KEY (plcidfp) ) INHERITS(place);"
  shp2pgsql -c -s 4269 -g the_geom -W "latin1" tl_2013_${NUMBER}_place.dbf tiger_staging.ny_place | $P
  $P -c "ALTER TABLE tiger_staging.${ABBREV}_place RENAME geoid TO plcidfp;"
  $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_place'), lower('${ABBREV}_place'));"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_place ADD CONSTRAINT uidx_${ABBREV}_place_gid UNIQUE (gid);"
  $P -c "CREATE INDEX idx_${ABBREV}_place_soundex_name ON tiger_data.${ABBREV}_place USING btree (soundex(name));" 
  $P -c "CREATE INDEX tiger_data_${ABBREV}_place_the_geom_gist ON tiger_data.${ABBREV}_place USING gist(the_geom);"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_place ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_cousub(CONSTRAINT pk_${ABBREV}_cousub PRIMARY KEY (cosbidfp), CONSTRAINT uidx_${ABBREV}_cousub_gid UNIQUE (gid)) INHERITS(cousub);" 
  shp2pgsql -c -s 4269 -g the_geom   -W "latin1" tl_2013_${NUMBER}_cousub.dbf tiger_staging.ny_cousub | $P
  $P -c "ALTER TABLE tiger_staging.${ABBREV}_cousub RENAME geoid TO cosbidfp;"
  $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_cousub'), lower('${ABBREV}_cousub'));"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_cousub ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX tiger_data_${ABBREV}_cousub_the_geom_gist ON tiger_data.${ABBREV}_cousub USING gist(the_geom);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_cousub_countyfp ON tiger_data.${ABBREV}_cousub USING btree(countyfp);"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_tract(CONSTRAINT pk_${ABBREV}_tract PRIMARY KEY (tract_id) ) INHERITS(tiger.tract);"
  shp2pgsql -c -s 4269 -g the_geom -W "latin1" tl_2013_${NUMBER}_tract.dbf tiger_staging.ny_tract | $P
  $P -c "ALTER TABLE tiger_staging.${ABBREV}_tract RENAME geoid TO tract_id;"
  $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_tract'), lower('${ABBREV}_tract'));"
  $P -c "CREATE INDEX tiger_data_${ABBREV}_tract_the_geom_gist ON tiger_data.${ABBREV}_tract USING gist(the_geom);"
  $P -c "VACUUM ANALYZE tiger_data.${ABBREV}_tract;"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_tract ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_tabblock(CONSTRAINT pk_${ABBREV}_tabblock PRIMARY KEY (tabblock_id)) INHERITS(tiger.tabblock);"
  shp2pgsql -c -s 4269 -g the_geom -W "latin1" tl_2013_${NUMBER}_tabblock.dbf tiger_staging.ny_tabblock | $P
  $P -c "ALTER TABLE tiger_staging.${ABBREV}_tabblock RENAME geoid TO tabblock_id;"
  $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_tabblock'), lower('${ABBREV}_tabblock'), '{gid, statefp10, countyfp10, tractce10, blockce10,suffix1ce,blockce,tractce}'::text[]);"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_tabblock ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX tiger_data_${ABBREV}_tabblock_the_geom_gist ON tiger_data.${ABBREV}_tabblock USING gist(the_geom);"
  $P -c "VACUUM ANALYZE tiger_data.${ABBREV}_tabblock;"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_bg(CONSTRAINT pk_${ABBREV}_bg PRIMARY KEY (bg_id)) INHERITS(tiger.bg);"
  shp2pgsql -c -s 4269 -g the_geom -W "latin1" tl_2013_${NUMBER}_bg.dbf tiger_staging.ny_bg | $P
  $P -c "ALTER TABLE tiger_staging.${ABBREV}_bg RENAME geoid TO bg_id;  SELECT loader_load_staged_data(lower('${ABBREV}_bg'), lower('${ABBREV}_bg'));"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_bg ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX tiger_data_${ABBREV}_bg_the_geom_gist ON tiger_data.${ABBREV}_bg USING gist(the_geom);"
  $P -c "VACUUM ANALYZE tiger_data.${ABBREV}_bg;"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_zcta5(CONSTRAINT pk_${ABBREV}_zcta5 PRIMARY KEY (zcta5ce,statefp), CONSTRAINT uidx_${ABBREV}_zcta5_gid UNIQUE (gid)) INHERITS(zcta5);"
  for z in *${NUMBER}_zcta510.dbf ; do
    shp2pgsql  -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.${ABBREV}_zcta510 | $P
    $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_zcta510'), lower('${ABBREV}_zcta5'));"
  done
  $P -c "ALTER TABLE tiger_data.${ABBREV}_zcta5 ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX tiger_data_${ABBREV}_zcta5_the_geom_gist ON tiger_data.${ABBREV}_zcta5 USING gist(the_geom);"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_faces(CONSTRAINT pk_${ABBREV}_faces PRIMARY KEY (gid)) INHERITS(faces);"
  for z in *faces.dbf; do 
    shp2pgsql  -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.${ABBREV}_faces | $P
    $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_faces'), lower('${ABBREV}_faces'));"
  done
  $P -c "CREATE INDEX tiger_data_${ABBREV}_faces_the_geom_gist ON tiger_data.${ABBREV}_faces USING gist(the_geom);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_faces_tfid ON tiger_data.${ABBREV}_faces USING btree (tfid);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_faces_countyfp ON tiger_data.${ABBREV}_faces USING btree (countyfp);"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_faces ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "VACUUM ANALYZE tiger_data.${ABBREV}_faces;"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_featnames(CONSTRAINT pk_${ABBREV}_featnames PRIMARY KEY (gid)) INHERITS(featnames);ALTER TABLE tiger_data.${ABBREV}_featnames ALTER COLUMN statefp SET DEFAULT '${NUMBER}';"
  for z in *featnames.dbf; do
    shp2pgsql  -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.${ABBREV}_featnames | $P
    $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_featnames'), lower('${ABBREV}_featnames'));"
  done
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_featnames_snd_name ON tiger_data.${ABBREV}_featnames USING btree (soundex(name));"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_featnames_lname ON tiger_data.${ABBREV}_featnames USING btree (lower(name));"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_featnames_tlid_statefp ON tiger_data.${ABBREV}_featnames USING btree (tlid,statefp);"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_featnames ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "VACUUM ANALYZE tiger_data.${ABBREV}_featnames;"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_edges(CONSTRAINT pk_${ABBREV}_edges PRIMARY KEY (gid)) INHERITS(edges);"
  for z in *edges.dbf; do
    shp2pgsql  -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.${ABBREV}_edges | $P
    $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_edges'), lower('${ABBREV}_edges'));"
  done
  $P -c "ALTER TABLE tiger_data.${ABBREV}_edges ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_edges_tlid ON tiger_data.${ABBREV}_edges USING btree (tlid);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_edgestfidr ON tiger_data.${ABBREV}_edges USING btree (tfidr);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_edges_tfidl ON tiger_data.${ABBREV}_edges USING btree (tfidl);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_edges_countyfp ON tiger_data.${ABBREV}_edges USING btree (countyfp);"
  $P -c "CREATE INDEX tiger_data_${ABBREV}_edges_the_geom_gist ON tiger_data.${ABBREV}_edges USING gist(the_geom);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_edges_zipl ON tiger_data.${ABBREV}_edges USING btree (zipl);"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_zip_state_loc(CONSTRAINT pk_${ABBREV}_zip_state_loc PRIMARY KEY(zip,stusps,place)) INHERITS(zip_state_loc);"
  $P -c "INSERT INTO tiger_data.${ABBREV}_zip_state_loc(zip,stusps,statefp,place) SELECT DISTINCT e.zipl, '${ABBREV}', '${NUMBER}', p.name FROM tiger_data.${ABBREV}_edges AS e INNER JOIN tiger_data.${ABBREV}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.${ABBREV}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_zip_state_loc_place ON tiger_data.${ABBREV}_zip_state_loc USING btree(soundex(place));"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_zip_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "vacuum analyze tiger_data.${ABBREV}_edges;"
  $P -c "vacuum analyze tiger_data.${ABBREV}_zip_state_loc;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_zip_lookup_base(CONSTRAINT pk_${ABBREV}_zip_state_loc_city PRIMARY KEY(zip,state, county, city, statefp)) INHERITS(zip_lookup_base);"
  $P -c "INSERT INTO tiger_data.${ABBREV}_zip_lookup_base(zip,state,county,city, statefp) SELECT DISTINCT e.zipl, '${ABBREV}', c.name,p.name,'${NUMBER}'  FROM tiger_data.${ABBREV}_edges AS e INNER JOIN tiger.county As c  ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '${NUMBER}') INNER JOIN tiger_data.${ABBREV}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.${ABBREV}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_zip_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_zip_lookup_base_citysnd ON tiger_data.${ABBREV}_zip_lookup_base USING btree(soundex(city));"

  ###
  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "CREATE SCHEMA tiger_staging;"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_addr(CONSTRAINT pk_${ABBREV}_addr PRIMARY KEY (gid)) INHERITS(addr);ALTER TABLE tiger_data.${ABBREV}_addr ALTER COLUMN statefp SET DEFAULT '${NUMBER}';"
  for z in *addr.dbf ; do
    shp2pgsql  -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.${ABBREV}_addr | $P
    $P -c "SELECT loader_load_staged_data(lower('${ABBREV}_addr'), lower('${ABBREV}_addr'));"
  done
  $P -c "ALTER TABLE tiger_data.${ABBREV}_addr ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_addr_least_address ON tiger_data.${ABBREV}_addr USING btree (least_hn(fromhn,tohn) );"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_addr_tlid_statefp ON tiger_data.${ABBREV}_addr USING btree (tlid, statefp);"
  $P -c "CREATE INDEX idx_tiger_data_${ABBREV}_addr_zip ON tiger_data.${ABBREV}_addr USING btree (zip);"
  $P -c "CREATE TABLE tiger_data.${ABBREV}_zip_state(CONSTRAINT pk_${ABBREV}_zip_state PRIMARY KEY(zip,stusps)) INHERITS(zip_state); "
  $P -c "INSERT INTO tiger_data.${ABBREV}_zip_state(zip,stusps,statefp) SELECT DISTINCT zip, '${ABBREV}', '${NUMBER}' FROM tiger_data.${ABBREV}_addr WHERE zip is not null;"
  $P -c "ALTER TABLE tiger_data.${ABBREV}_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '${NUMBER}');"
  $P -c "VACUUM ANALYZE tiger_data.${ABBREV}_addr;"

  $P -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
  $P -c "SELECT install_missing_indexes();"
  $P -c "SELECT missing_indexes_generate_script();"
}

for abbrev in $# ; do
  create_state $abbev
done

rm -fr "$TMP"

