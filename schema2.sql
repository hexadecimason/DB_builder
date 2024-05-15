-- SCHEMA:

-- Well(api, operator, lease, well_num, ...)
-- File(file_num, sample_type, box_count, ...)
-- Box(file_num , box_num, top, bottom, ...)
-- well_file(file_num, api)

-- RP_Sample(api, depth, etc.)

DROP TABLE IF EXISTS RP_Sample;
DROP TABLE IF EXISTS well_file;
DROP TABLE IF EXISTS Box;
DROP TABLE IF EXISTS Well;
DROP TABLE IF EXISTS File;

DROP TABLE IF EXISTS changelog;
DROP TABLE IF EXISTS noAPI;

DROP VIEW IF EXISTS xl_grid;

DROP TRIGGER IF EXISTS log_update;
DROP TRIGGER IF EXISTS log_deletion;
DROP TRIGGER IF EXISTS log_insertion;

-- PRIMARY TABLES: 

CREATE TABLE Well (
    api        int PRIMARY KEY,
    operator   text NOT NULL,
    lease      text NOT NULL,
    well_num   text NOT NULL,
    sec        int,
    twn        int,
    twn_d      text,
    rng        int,
    rng_d      text,
    qq         text,
    lat        float,
    long       float,
    county     text,
    state      text,
    field      text
);

CREATE TABLE File (
    file_num     text PRIMARY KEY,
    collection   text NOT NULL,
    sample_type  text,
    box_count    int,
    box_type     text,
    diameter     text,
    location     text,
    comments     text
);

CREATE TABLE Box (
    file_num  text,
    box_num   int,
    top       int,
    bottom    int,
    formation text,
    condition text,
    comments  text,

    FOREIGN KEY (file_num) REFERENCES File
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    PRIMARY KEY (file_num, box_num)
);

CREATE TABLE well_file (
    api        int,
    file_num   int PRIMARY KEY,

    FOREIGN KEY (api) REFERENCES Well
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    FOREIGN KEY (file_num) REFERENCES File
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE changelog(
    datetime     datetime,
    type         text,
    change       text,

    PRIMARY KEY (datetime, type, change)
);


-- TODO:
CREATE TABLE RP_Sample(
    api         int,
    depth       real,
);

-- TODO:
CREATE TABLE noAPI(

);

-- VIEWS:

CREATE VIEW IF NOT EXISTS xl_grid
    AS
    SELECT Well.api, operator, lease, well_num, sec,
        twn, twn_d, rng, rng_d, qq, lat, long, county, state, field,
        File.file_num, collection, sample_type, box_count, box_type,
        diameter, location, box_num, top, bottom, formation,
        condition, comments
    FROM Well, well_file, File, Box
    WHERE Well.api = well_file.api
    AND well_file.file_num = File.file_num
    AND File.file_num = Box.file_num;

-- TRIGGERS (logging):
-- Not every query needs logged, as much of the data can be verified with scout tickets and what's on the shelf.

CREATE TRIGGER log_filenum_change
AFTER UPDATE ON File.file_num
BEGIN
    INSERT INTO changelog
    VALUES (current_timestamp, 'EDIT file_num', OLD.file_num || ' -> ' || NEW.file_num)
END;

CREATE TRIGGER log_file_delete
AFTER DELETE ON File
BEGIN
    INSERT INTO changelog
    VALUES(current_timestamp, 'DELETE File', OLD.file_num);
END;

