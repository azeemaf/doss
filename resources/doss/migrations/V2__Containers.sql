create table containers (
	container_id bigint primary key auto_increment,
	area varchar(4000) not null,
	size bigint not null default 0,
	sealed boolean not null default 0);