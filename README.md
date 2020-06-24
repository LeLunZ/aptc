# APTC - The austrian public transport crawler

## What is it?
APTC is a crawler which should get all the public transport timetables of austria. 
These timetables are then exported in the [GTFS Format](https://developers.google.com/transit/gtfs).

## Why do we need this?
Even tough the [ÖBB](https://www.oebb.at) has their own [open data feed](https://data.oebb.at), we need all the public transport data not only the one from ÖBB. 

## Usage
The Database Tables have to exist before running the crawler! These dont get created when building with Docker.

Then you need a `bus_stops.csv` file. Which you need to locate in the `src/Data` folder. These stops will be crawled.

When starting the container without options, the database needs to be located at `localhost:5432/postgres`:
`docker-compose up
`
### Environment Variables
Please add these to docker via the docker-compose or the docker run command.

- `postgres` specify your database connection string. Which should look like this: `username:password@url:port/database`
- `export` set Export to `TRUE` to export all the data. If this variable is specified the crawler won't crawl!
- `csvbegin` sets the line in the csv where the crawler will beginn to crawl. If not specified it will start at the beginning and will skip one header line. 
- `csvend` sets the line in the csv where the crawler will end to crawl. If not specified it will crawl to the end.
- `continues` if set to TRUE the crawler will append all crawled stops to the file and also crawl the stops which arent in the csv.


### Bugs
Careful: if you're running postgres locally on a mac, don't use localhost, but `docker.for.mac.host.internal` instead
`docker-compose run -e postgres=postgresUser:passMe@docker.for.mac.host.internal:5432/postgresDB app`


# Export Data
To get all the crawled data just run another docker container with the export statement:
`docker-compose run -e export=TRUE app`

All the Data will be in the `db` folder.