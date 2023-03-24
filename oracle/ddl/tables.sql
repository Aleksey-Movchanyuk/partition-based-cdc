-- create a table for countries
CREATE TABLE country (
  country_code VARCHAR2(10) PRIMARY KEY,
  country_name VARCHAR2(50)
);

-- create a table for currencies
CREATE TABLE currency (
  currency_code VARCHAR2(10) PRIMARY KEY,
  currency_name VARCHAR2(50),
  currency_symbol VARCHAR2(10)
);

-- create a table for clients
CREATE TABLE client (
  client_id NUMBER PRIMARY KEY,
  first_name VARCHAR2(50),
  last_name VARCHAR2(50),
  email VARCHAR2(100),
  phone_number VARCHAR2(50),
  date_of_birth DATE,
  address VARCHAR2(200),
  country_code VARCHAR2(10),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT systimestamp,
  CONSTRAINT fk_client_country FOREIGN KEY (country_code)
    REFERENCES country(country_code)
);

-- create a table for cards
CREATE TABLE card (
  card_id NUMBER PRIMARY KEY,
  client_id NUMBER,
  card_number VARCHAR2(16),
  expiration_date DATE,
  cvv VARCHAR2(3),
  status VARCHAR2(20),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT systimestamp,
  CONSTRAINT fk_card_client FOREIGN KEY (client_id)
    REFERENCES client(client_id)
);

-- create a table for accounts
CREATE TABLE account (
  account_id NUMBER PRIMARY KEY,
  client_id NUMBER,
  currency_code VARCHAR2(10),
  balance NUMBER(10,2),
  status VARCHAR2(20),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT systimestamp,
  CONSTRAINT fk_account_client FOREIGN KEY (client_id)
    REFERENCES client(client_id),
  CONSTRAINT fk_account_currency FOREIGN KEY (currency_code)
    REFERENCES currency(currency_code)
);

-- create a partitioned table for transactions
CREATE TABLE transaction (
  transaction_id VARCHAR2(100) PRIMARY KEY,
  card_id NUMBER,
  account_id NUMBER,
  amount NUMBER(10,2),
  description VARCHAR2(200),
  transaction_ts TIMESTAMP WITH TIME ZONE DEFAULT systimestamp,
  transaction_dt DATE GENERATED ALWAYS AS (TRUNC(transaction_ts)) VIRTUAL,
  CONSTRAINT fk_transaction_card FOREIGN KEY (card_id)
    REFERENCES card(card_id),
  CONSTRAINT fk_transaction_account FOREIGN KEY (account_id)
    REFERENCES account(account_id)
)
PARTITION BY RANGE (transaction_dt)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
  PARTITION transaction_2022_12_31 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD'))
);
