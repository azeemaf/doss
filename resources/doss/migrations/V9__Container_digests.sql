create table if not exists container_digests (
	container_id BIGINT,
	algorithm VARCHAR(8),
	digest VARCHAR(512),
	PRIMARY KEY (container_id, algorithm));

insert into container_digests (container_id,algorithm,digest) select container_id,sha1,'sha1' from containers;
alter table containers drop column sha1;

