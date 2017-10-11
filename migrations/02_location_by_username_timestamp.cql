CREATE TABLE location_by_timestamp (
    username text,
    timestamp_created timestamp,
    longitude float,
    latitude float,
    location_name text,
    PRIMARY KEY (username, timestamp_created)
);

--//@UNDO

DROP TABLE location_by_timestamp;
