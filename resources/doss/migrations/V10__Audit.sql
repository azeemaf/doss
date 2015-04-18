create table if not exists digest_audits (
        container_id BIGINT,
        algorithm VARCHAR(8),
        time TIMESTAMP,
        result BOOLEAN);
create index aud_time_desc on digest_audits(time desc);


