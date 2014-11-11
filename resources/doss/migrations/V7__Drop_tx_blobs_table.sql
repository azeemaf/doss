ALTER TABLE blobs ADD tx_id bigint;
UPDATE blobs SET tx_id = SELECT tx_blobs.tx_id FROM tx_blobs WHERE tx_blobs.blob_id = blobs.blob_id;
DROP TABLE tx_blobs;

ALTER TABLE txs ADD opened TIMESTAMP;
ALTER TABLE txs ADD closed TIMESTAMP;
