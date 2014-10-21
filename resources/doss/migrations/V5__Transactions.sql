CREATE TABLE txs (
    tx_id BIGINT PRIMARY KEY,
    state INTEGER NOT NULL,
    client VARCHAR(4000),
);

CREATE TABLE tx_blobs (
    tx_id BIGINT NOT NULL,
    blob_id BIGINT NOT NULL,
);