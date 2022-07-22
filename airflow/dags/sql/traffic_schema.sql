CREATE TABLE  IF NOT EXISTS traffic (
    track_id numeric, 
    type text not null, 
    traveled_d double precision DEFAULT NULL,
    avg_speed double precision DEFAULT NULL, 
    lat double precision DEFAULT NULL, 
    lon double precision DEFAULT NULL, 
    speed double precision DEFAULT NULL,    
    lon_acc double precision DEFAULT NULL, 
    lat_acc double precision DEFAULT NULL, 
    time double precision DEFAULT NULL
);