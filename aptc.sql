create table agency
(
    agency_id       text
        constraint agency_agency_id_key
            unique,
    agency_name     text,
    agency_url      text,
    agency_timezone text,
    agency_lang     text,
    agency_phone    text
);

create table stops
(
    stop_id             integer not null
        constraint stops_pkey
            primary key,
    stop_code           text
        constraint stops_stop_code_key
            unique,
    stop_name           text    not null,
    stop_desc           text,
    stop_lat            wgs84_lat,
    stop_lon            wgs84_lon,
    zone_id             text,
    stop_url            text,
    location_type       integer,
    parent_station      text,
    wheelchair_boarding text
);

create table routes
(
    agency_id        text,
    route_short_name text not null,
    route_long_name  text,
    route_desc       text,
    route_type       integer,
    route_url        text,
    route_color      text,
    route_text_color text,
    route_id         text not null
        constraint routes_pkey
            primary key
);

create table calendar
(
    service_id text       not null
        constraint calendar_pkey
            primary key,
    monday     boolean    not null,
    tuesday    boolean    not null,
    wednesday  boolean    not null,
    thursday   boolean    not null,
    friday     boolean    not null,
    saturday   boolean    not null,
    sunday     boolean    not null,
    start_date numeric(8),
    end_date   numeric(8) not null
);

create index calendar_end_date_start_date_monday_tuesday_wednesday_thursday_
    on calendar (end_date, start_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday);

create table shapes
(
    shape_id            text,
    shape_pt_sequence   integer   not null,
    shape_dist_traveled double precision,
    shape_pt_lat        wgs84_lat not null,
    shape_pt_lon        wgs84_lon not null
);

create table trips
(
    route_id              text   not null,
    service_id            text,
    trip_short_name       text,
    trip_headsign         text,
    direction_id          boolean,
    block_id              text,
    shape_id              text,
    wheelchair_accessible text,
    trip_id               serial not null
        constraint trips_pkey
            primary key
);

create table frequencies
(
    trip_id      text     not null,
    start_time   interval not null,
    end_time     interval not null,
    headway_secs integer  not null,
    exact_times  text
);

create table transfers
(
    from_stop_id  text    not null,
    to_stop_id    text    not null,
    transfer_type integer not null
);

create table stop_times
(
    trip_id             integer not null,
    stop_sequence       integer not null,
    stop_id             text    not null,
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
    constraint stop_times_pk
        primary key (trip_id, stop_id, stop_sequence)
);