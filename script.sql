CREATE TABLE transactions (
    transaction_id serial NOT NULL,
    date_and_time timestamp,
    branch_name character(255),
    payment_type integer,
    total_cost numeric,
    PRIMARY KEY (transaction_id)
);

CREATE TABLE basket_item (
    bi_id serial NOT NULL,
    bi_size character(255),
    bi_name character(255),
    bi_cost numeric,
    transaction_id integer,
    PRIMARY KEY (bi_id),
    FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id)
);