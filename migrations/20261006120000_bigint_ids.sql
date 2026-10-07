-- SERIAL ids are 32-bit (max 2,147,483,647), and the sequence never goes backwards when the
-- normalizer deletes processed raw rows - so at L3/book message rates raw_financial_data
-- eventually runs out of ids and EVERY insert fails. Widen each id column and its sequence
-- to 64-bit. (The normalized tables get one row per data item, so they grow even faster.)
--
-- NOTE: ALTER COLUMN ... TYPE rewrites the whole table under an ACCESS EXCLUSIVE lock.
-- raw_financial_data is drained hourly so it's quick; the *_normalized tables may take a
-- while if they're large. Inserts block (they don't fail) for the duration.
ALTER TABLE raw_financial_data ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE raw_financial_data_id_seq AS BIGINT;

ALTER TABLE trades_normalized ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE trades_normalized_id_seq AS BIGINT;

ALTER TABLE book_normalized ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE book_normalized_id_seq AS BIGINT;

ALTER TABLE orders_normalized ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE orders_normalized_id_seq AS BIGINT;
