CREATE TABLE processed_transactions
(
    transaction_id       VARCHAR(100) PRIMARY KEY,
    merchant_id          VARCHAR(100) NOT NULL,
    amount               NUMERIC(38, 2) NOT NULL,
    currency             CHAR(3) NOT NULL,
    status               VARCHAR(20) NOT NULL,
    timestamp            TIMESTAMPTZ NOT NULL,
    processed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE merchant_aggregates
(
    merchant_id VARCHAR(100) NOT NULL,
    currency CHAR(3) NOT NULL,
    successful_transaction_count BIGINT NOT NULL,
    successful_transaction_amount NUMERIC(38, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT pk_merchant_aggregates
        PRIMARY KEY (merchant_id, currency),

    CONSTRAINT ck_successful_count_non_negative
        CHECK (successful_transaction_count >= 0),

    CONSTRAINT ck_successful_amount_non_negative
        CHECK (successful_transaction_amount >= 0)
);