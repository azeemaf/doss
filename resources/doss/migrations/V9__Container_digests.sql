create table if not exists container_digests (
	container_id BIGINT,
	algorithm VARCHAR(8),
	digest VARCHAR(512),
	PRIMARY KEY (container_id, algorithm));

alter table containers drop column sha1;

