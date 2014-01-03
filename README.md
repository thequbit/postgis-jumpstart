# Jumpstart a database with working geocoding features that you would normally get from the tiger geocoder mess.

Wow.  The "install" process for getting postgis to do all the things is horrible.  Really, horrible.

0. You need wget, subversion, unzip.
1. Install postgres 9.3 something.
2. Install postgis 2.1 something.
3. Run
    make-template-db.sh <state-abbreviation> [more state-abbreviatons...]
4. Run
    make-gis-db-from-template.sh <name-of-new-database>
    # or, if you want a new user for all the gis things.
    make-gis-db-from-template-with-new-user.sh <name-of-new-database> <name-of-new-user>



