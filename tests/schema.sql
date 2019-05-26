CREATE TABLE data(
       id serial PRIMARY KEY,
       username integer UNIQUE NOT NULL,
       created_on timestamp NOT NULL DEFAULT NOW()
);
