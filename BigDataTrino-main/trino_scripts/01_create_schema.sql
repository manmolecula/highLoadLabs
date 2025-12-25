-- Dimension: Customers
CREATE TABLE IF NOT EXISTS clickhouse.default.dim_customer (
    customer_id integer,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    age integer,
    email varchar,
    country varchar,
    postal_code varchar,
    pet_type varchar,
    pet_name varchar,
    pet_breed varchar,
    pet_category varchar,
    created_at DATE
);



-- Dimension: Sellers
CREATE TABLE IF NOT EXISTS clickhouse.default.dim_seller (
    seller_id integer,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    email varchar,
    country varchar,
    postal_code varchar,
    created_at DATE
    );


-- Dimension: Products
CREATE TABLE IF NOT EXISTS clickhouse.default.dim_product (
    product_id integer,
    name varchar NOT NULL,
    category varchar,
    price double,
    weight double,
    color varchar,
    size varchar,
    brand varchar,
    material varchar,
    description varchar,
    rating real,
    reviews integer,
    release_date date,
    expiry_date date,
    created_at DATE
);


-- Dimension: Stores
CREATE TABLE IF NOT EXISTS clickhouse.default.dim_store (
    name varchar NOT NULL,
    location varchar,
    city varchar,
    state varchar,
    country varchar,
    phone varchar,
    email varchar,
    created_at DATE
);


-- Dimension: Suppliers
CREATE TABLE IF NOT EXISTS clickhouse.default.dim_supplier (
    name varchar NOT NULL,
    contact varchar,
    email varchar,
    phone varchar,
    address varchar,
    city varchar,
    country varchar,
    created_at DATE
);


-- Fact: Sales
CREATE TABLE IF NOT EXISTS clickhouse.default.fact_sales (
    sale_date date NOT NULL,
    customer_id integer NOT NULL,
    seller_id integer NOT NULL,
    product_id integer NOT NULL,
    store_name varchar,
    supplier_name varchar,
    quantity integer NOT NULL,
    total_price double NOT NULL,
    created_at DATE
);
