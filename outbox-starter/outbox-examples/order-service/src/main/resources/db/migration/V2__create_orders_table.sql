CREATE TABLE orders (
    id              VARCHAR(36)     PRIMARY KEY,
    customer_email  VARCHAR(255)    NOT NULL,
    product_name    VARCHAR(255)    NOT NULL,
    amount          NUMERIC(12,2)   NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'CREATED',
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
