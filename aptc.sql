create domain wgs84_lat as double precision
    constraint wgs84_lat_check check ((VALUE >= ('-90'::integer)::double precision) AND
                                      (VALUE <= (90)::double precision));

create domain wgs84_lon as double precision
    constraint wgs84_lon_check check ((VALUE >= ('-180'::integer)::double precision) AND
                                      (VALUE <= (180)::double precision));

create domain gtfstime as text
    constraint gtfstime_check check (VALUE ~ '^[0-9]?[0-9]:[0-5][0-9]:[0-5][0-9]$'::text);

create table agency
(
    agency_id       serial not null
        constraint agency_pk
            primary key,
    agency_name     text,
    agency_url      text,
    agency_timezone text,
    agency_lang     text,
    agency_phone    text
);

create unique index agency_agency_name_uindex
    on agency (agency_name);

create table stops
(
    stop_id             serial not null
        constraint stops_pkey
            primary key,
    stop_code           text,
    stop_name           text   not null,
    stop_desc           text,
    stop_lat            double precision,
    stop_lon            double precision,
    zone_id             text,
    stop_url            text,
    location_type       integer,
    parent_station      text,
    wheelchair_boarding text
);

create unique index stops_stop_name_uindex
    on stops (stop_name);

create table routes
(
    agency_id        integer,
    route_short_name text,
    route_long_name  text,
    route_desc       text,
    route_type       integer,
    route_url        text,
    route_color      text,
    route_text_color text,
    route_id         serial not null
        constraint routes_pkey
            primary key,
    constraint routes_sk
        unique (agency_id, route_long_name, route_type)
);

create table calendar
(
    service_id          serial     not null
        constraint calendar_pkey
            primary key,
    monday              boolean    not null,
    tuesday             boolean    not null,
    wednesday           boolean    not null,
    thursday            boolean    not null,
    friday              boolean    not null,
    saturday            boolean    not null,
    sunday              boolean    not null,
    start_date          numeric(8),
    end_date            numeric(8) not null,
    calendar_dates_hash BIGINT
);

create unique index calendar_end_date_start_date_monday_tuesday_hash
    on calendar (end_date, start_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
                 calendar_dates_hash);

create table shapes
(
    shape_id            serial    not null
        constraint shapes_pk
            primary key,
    shape_pt_sequence   integer   not null,
    shape_dist_traveled double precision,
    shape_pt_lat        wgs84_lat not null,
    shape_pt_lon        wgs84_lon not null
);

create table trips
(
    route_id                    integer not null,
    service_id                  integer,
    trip_short_name             text,
    trip_headsign               text,
    direction_id                boolean,
    block_id                    text,
    shape_id                    text,
    wheelchair_accessible       text,
    trip_id                     serial  not null
        constraint trips_pkey
            primary key,
    oebb_url                    text,
    station_departure_time text    not null
);

create unique index trips_service_id_route_id_station_departure_time_uindex
    on trips (service_id, route_id, station_departure_time);

create table frequencies
(
    trip_id      integer  not null,
    start_time   interval not null,
    end_time     interval not null,
    headway_secs integer  not null,
    exact_times  text
);

create table transfers
(
    from_stop_id  integer not null,
    to_stop_id    integer not null
        constraint transfers_pk
            primary key,
    transfer_type integer not null
);

create table stop_times
(
    trip_id             integer not null,
    stop_sequence       integer not null,
    stop_id             integer not null,
    arrival_time        text    not null,
    departure_time      text    not null,
    stop_headsign       text,
    pickup_type         integer
        constraint stop_times_pickup_type_check
            check ((pickup_type >= 0) AND (pickup_type <= 3)),
    drop_off_type       integer
        constraint stop_times_drop_off_type_check
            check ((drop_off_type >= 0) AND (drop_off_type <= 3)),
    shape_dist_traveled double precision,
    stop_times_id       serial  not null
        constraint stop_times_pk
            primary key
);

create table calendar_dates
(
    service_id     integer not null,
    date           integer not null,
    exception_type integer not null,
    constraint calendar_dates_pk
        primary key (service_id, date, exception_type)
);

create table transport_type_image
(
    name     text not null
        constraint transport_type_image_pk
            primary key,
    oebb_url text
);

create unique index transport_type_image_name_uindex
    on transport_type_image (name);

create table stop_time_text
(
    working_days text not null
        constraint stop_time_text_pk
            primary key
);

