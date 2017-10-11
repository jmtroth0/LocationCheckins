CREATE TABLE location (
    username text,
    timestamp_created timestamp,
    longitude float,
    latitude float,
    location_name text,
    PRIMARY KEY (username)
);

--//@UNDO

DROP TABLE location;
