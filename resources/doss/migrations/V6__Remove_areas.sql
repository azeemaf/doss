ALTER TABLE containers DROP COLUMN sealed;
ALTER TABLE containers DROP COLUMN area;
ALTER TABLE containers ADD state INTEGER;
ALTER TABLE containers ADD sha1 VARCHAR(512);