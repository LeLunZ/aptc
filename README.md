# APTC - The austrian public transport crawler

## usage
The Database Tables have to exist before running the crawler!

When starting the container without option, the database needs to be located at localhost:5432/postgres

`docker-compose up
`

if you want to change the uri use the environment variable `postgres` with this pattern `username:password@url:port/database`

example:

`docker-compose run -e postgres=postgresUser:passMe@localhost:5432/postgresDB app`

Careful: if you're running postgres locally on a mac, don't use localhost, but `docker.for.mac.host.internal` instead
`docker-compose run -e postgres=postgresUser:passMe@docker.for.mac.host.internal:5432/postgresDB app`

For changing the location of the crawler to start and to end in the .csv file pass it through `csvbegin` and `csvend`

example:

`docker-compose run -e csvbegin=3 -e csvend=10 app`


# Export

`docker-compose run -e export=TRUE app`

fährt täglich
und Station/Stop_type
http://fahrplan.oebb.at/bin/traininfo.exe/dn/795198/569564/392280/68926/181?date=13.01.2020&dcArrSt=M%C3%BCllendorf%20Bahnhof&trainname=REX%207624&stationname=M%FCllendorf%20Bahnhof&stationFilter=81,01,02,03,04,05,06,07,08,09&backLink=ts&

http://fahrplan.oebb.at/bin/traininfo.exe/dn/795198/571906/392280/68927/181?date=13.01.2020&dcArrSt=Eisenstadt%20Finanzamt&trainname=REX%202615&stationname=Eisenstadt%20Finanzamt&stationFilter=81,01,02,03,04,05,06,07,08,09&backLink=ts&
