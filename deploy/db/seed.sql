CREATE TABLE accounts (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    balance     numeric(12,2) NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id          bigserial PRIMARY KEY,
    account_id  int NOT NULL REFERENCES accounts(id),
    amount      numeric(12,2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_account_idx ON orders(account_id);

INSERT INTO accounts(name, balance)
SELECT 'user_' || g, 100 FROM generate_series(1, 1000) g;

INSERT INTO orders(account_id, amount)
SELECT (random()*999)::int + 1, (random()*500)::numeric(12,2)
FROM generate_series(1, 200000);