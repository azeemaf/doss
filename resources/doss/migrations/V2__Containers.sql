create table containers (
	container_id bigint primary key autoincrement,
	area not null varchar(4000),
	size bigint not null default 0,
	sealed boolean not null default 0);