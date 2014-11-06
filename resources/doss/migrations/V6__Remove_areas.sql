ALTER TABLE containers DROP COLUMN sealed;
ALTER TABLE containers DROP COLUMN area;
ALTER TABLE containers ADD state INTEGER;