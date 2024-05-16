-- #TODO: Create new TS hypertable
CREATE TABLE IF NOT EXISTS sensor_data (
    id integer NOT NULL, 
    temperature float, 
    humidity float, 
    velocity float, 
    battery_level float NOT NULL, 
    last_seen timestamp NOT NULL,
    PRIMARY KEY (id, last_seen)
);

SELECT create_hypertable('sensor_data', 'last_seen',if_not_exists => true);
CREATE UNIQUE INDEX time ON sensor_data(id, last_seen)