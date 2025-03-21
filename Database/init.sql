CREATE TABLE fraud_cases (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255),
    amount FLOAT,
    hour INT,
    day_of_week INT,
    distance FLOAT,
    is_fraud INT,
    prediction INT,
    probability FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);