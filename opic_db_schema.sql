-- SCHEMA:

-- Well(api, operator, lease, well_num, ...)
-- File(file_num, sample_type, box_count, ...)
-- Box(file_num , box_num, top, bottom, ...)
-- well_file(file_num, api)

DROP TABLE IF EXISTS well_file;
DROP TABLE IF EXISTS Box;
DROP TABLE IF EXISTS Well;
DROP TABLE IF EXISTS File;

DROP VIEW IF EXISTS xl_grid;

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
    location     text
);

CREATE TABLE Box (
    file_num  text,
    box_num   int,
    top       int,
    bottom    int,
    formation text,
    condition text,
    comments  text,

    FOREIGN KEY (file_num) REFERENCES File,
    PRIMARY KEY (file_num, box_num)
);

CREATE TABLE well_file (
    api        int NOT NULL,
    file_num   int PRIMARY KEY,

    FOREIGN KEY (api) REFERENCES Well,
    FOREIGN KEY (file_num) REFERENCES File

);

-- VIEWS:

CREATE VIEW IF NOT EXISTS xl_grid
    AS
    SELECT Well.api, File.file_num, operator, well_num, lease,
        box_num, top, bottom, formation, comments
    FROM Well, well_file, File, Box
    WHERE Well.api = well_file.api
    AND well_file.file_num = File.file_num
    AND File.file_num = Box.file_num;

