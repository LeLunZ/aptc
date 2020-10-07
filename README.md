# APTC - The austrian public transport crawler

## What is it?
APTC is a crawler which should get all the public transport timetables of austria. 
These timetables are then exported in the [GTFS Format](https://developers.google.com/transit/gtfs).

## Why do we need this?
Even tough the [ÖBB](https://www.oebb.at) has their own [open data feed](https://data.oebb.at), we need all the public transport data not only the one from ÖBB. 

## Usage
The Database Tables have to exist before running the crawler! These dont get created when building with Docker.

Then you need a `bus_stops.csv` file. Which you need to locate in the `src/Data` folder. These stops will be crawled or more (see config.json).

When starting the container without options, the database needs to be located at `localhost:5432/postgres`:
`docker-compose up
`

### Environment Variables
Please add these to docker via the docker-compose or the docker run command.

- `CRAWLER_CONFIG` the name of your config (see config.json). if not specified its `default`.
- `AM_I_IN_A_DOCKER_CONTAINER` needs to be set to true if you run the code in a docker container.

### config.json
In the `config.json` located at the Data folder you can specify some options.
The top level key in the json object defines your config name. So you can add multiple configs.

- `postgres` | string | specify your database connection string. Which should look like this: `username:password@url:port/database` 
- `export` | boolean | exports all the data after crawling. 
- `crawl` | boolean | crawls data.
- `continues` | boolean | crawls not only data defined in the csv sheet. 
- `batchSize` | int | Size of csv lines the crawler crawls at one time. somewhere between 10-50 is recommended.
- `dates` | array | An array with all dates to crawl. Dateformat: "dd.mm.YYYY"
- `csv` | object | sets options for the csv sheet.
- `csv.begin` | int | sets the line in the csv where the crawler will beginn to crawl. If not specified it will start at the beginning and will skip one header line. 
- `csv.end` | int | sets the line in the csv where the crawler will end to crawl. If not specified it will crawl to the end.
- `crawlStopOptions` | object | Sets options for crawling stops.

If `crawlStopOptions` is defined you must define these values:
```
"crawlStopOptions": {
      "northLatBorder": 48.325,
      "southLatBorder": 48.115,
      "westLonBorder": 16.177,
      "eastLonBorder": 16.582
    }
```  
The Border values are defining are rectangle where the crawler will crawl the transit data. Routes which begin or end in this rectangle and go out will still be crawled.


### Bugs
Careful: if you're running postgres locally on a mac, don't use localhost, but `docker.for.mac.host.internal` instead
`docker-compose run -e postgres=postgresUser:passMe@docker.for.mac.host.internal:5432/postgresDB app`


# Export Data
To get all the crawled data just run another docker container with the export statement:
`docker-compose run -e export=TRUE app

All the Data will be in the `db` folder.


# Future Stuff

while using shapefiles currently we only check if coordinates are in the first shape.