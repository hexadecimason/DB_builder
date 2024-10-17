DROP TABLE IF EXISTS Sample;
DROP TABLE IF EXISTS well_file;
DROP TABLE IF EXISTS Box;
DROP TABLE IF EXISTS Well;
DROP TABLE IF EXISTS File;

DROP TABLE IF EXISTS changelog;

DROP VIEW IF EXISTS xl_grid_full;
DROP VIEW IF EXISTS xl_grid_basic;
DROP VIEW IF EXISTS rp_grid;
DROP VIEW IF EXISTS photofile_grid;

DROP TRIGGER IF EXISTS log_filenum_change;
DROP TRIGGER IF EXISTS log_file_delete;

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
    lat        real,
    long       real,
    county     text,
    state      text,
    field      text,
    img_files  text
);

CREATE TABLE File (
    file_num        text PRIMARY KEY,
    collection      text NOT NULL,
    sample_type     text,
    box_count       int,
    box_type        text,
    diameter        text,
    location        text,
    file_comments   text
);

-- This is a weak entity set
CREATE TABLE Box (
    file_num        text NOT NULL,
    box_num         int NOT NULL,
    top             real,
    bottom          real,
    formation       text,
    condition       text,
    box_comments    text,

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

-- This is a weak entity set
CREATE TABLE Sample (
    api             int NOT NULL,
    depth           real NOT NULL,
    sample_type     text,
    verified        date,
    description     text,
    box_id          text,

    FOREIGN KEY (api) REFERENCES Well
        ON UPDATE CASCADE,
    PRIMARY KEY (api, depth)
);

CREATE TABLE changelog (
    datetime     datetime,
    type         text,
    change       text,

    PRIMARY KEY (datetime, type, change)
);

-- VIEWS:

CREATE VIEW xl_grid_full
    AS
    SELECT File.file_num, location, collection, box_num, box_count, Well.api, operator, lease, well_num,
        sec, twn, twn_d, rng, rng_d, qq, lat, long, county, state, field,
        formation, top, bottom, sample_type, box_type, condition,
        diameter, file_comments, box_comments
    FROM Well, well_file, File, Box
    WHERE Well.api = well_file.api
    AND well_file.file_num = File.file_num
    AND File.file_num = Box.file_num;

CREATE VIEW xl_grid_basic
    AS
    SELECT File.file_num, location, colleciton, box_num, Well.api, operator, lease, well_num, 
        county, state, formation, top, bottom, sample_type, box_type, condition,
        file_comments, box_comments
    FROM Well, well_file, File, Box
    WHERE Well.api = well_file.api
    AND well_file.file_num = File.file_num
    AND File.file_num = Box.file_num;

CREATE VIEW rp_grid
    AS
    SELECT box_id, Well.api, operator, lease, well_num, county, state,
        depth, sample_type, description, verified
    FROM Well, Samples
    WHERE Well.api = Samples.api;

CREATE VIEW photofile_grid
    AS
    SELECT api, operator lease, well_num, county, img_files
    FROM Well;

-- TRIGGERS (logging):
-- Not every query needs logged, as much of the data can be verified with scout tickets and what's on the shelf.

CREATE TRIGGER log_filenum_change
AFTER UPDATE ON File
WHEN OLD.file_num != NEW.file_num
BEGIN
    INSERT INTO changelog
    VALUES (current_timestamp, 'EDIT file_num', OLD.file_num || ' -> ' || NEW.file_num);
END;

CREATE TRIGGER log_file_delete
AFTER DELETE ON File
BEGIN
    INSERT INTO changelog
    VALUES(current_timestamp, 'DELETE File', OLD.file_num);
END;