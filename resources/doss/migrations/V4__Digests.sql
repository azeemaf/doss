create table if not exists digests (
	blob_id BIGINT,
	algorithm VARCHAR(8),
	digest VARCHAR(512),
	PRIMARY KEY (blob_id, algorithm));
