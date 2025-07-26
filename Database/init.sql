CREATE TABLE IF NOT EXISTS all_transactions (
    trans_num VARCHAR(255) PRIMARY KEY,
    trans_date_trans_time TIMESTAMP,
    cc_num VARCHAR(255),
    merchant VARCHAR(255),
    category VARCHAR(255),
    amt NUMERIC(10, 2),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(10),
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(10),
    zip INTEGER,
    lat NUMERIC(10, 6),
    long NUMERIC(10, 6),
    city_pop INTEGER,
    job VARCHAR(255),
    dob DATE,
    unix_time BIGINT,
    merch_lat NUMERIC(10, 6),
    merch_long NUMERIC(10, 6),
    is_fraud INTEGER
);

CREATE TABLE IF NOT EXISTS fraud_transactions (
    trans_num VARCHAR(255) UNIQUE,
    amt NUMERIC(10, 2),
    category VARCHAR(255),
    actual_fraud INTEGER,
    fraud_probability NUMERIC(5, 4),
    predicted_label NUMERIC(1, 0),
    CONSTRAINT fake_transactions
        FOREIGN KEY (trans_num)
        REFERENCES all_transactions (trans_num)
);