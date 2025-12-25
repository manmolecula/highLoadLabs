CREATE TABLE kafka_source_events (
    id INT,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price DECIMAL(10,2),
    product_quantity INT,
    sale_date STRING,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price DECIMAL(10,2),
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    pet_category STRING,
    product_weight DECIMAL(10,2),
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating DECIMAL(3,2),
    product_reviews INT,
    product_release_date STRING,
    product_expiry_date STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'source_events_v2',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_consumer_final',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset',
    'properties.auto.offset.reset' = 'earliest',
    'json.ignore-parse-errors' = 'true'
);


CREATE TABLE dim_customer (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50),
    pet_category VARCHAR(50),
    created_at TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3_db_postgres',
    'table-name' = 'dim_customer',
    'username' = 'user',
    'password' = 'user',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_seller (
    seller_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(50),
    created_at TIMESTAMP(3),
    PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3_db_postgres',
    'table-name' = 'dim_seller',
    'username' = 'user',
    'password' = 'user',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_product (
    product_id INT,
    name VARCHAR(100),
    category VARCHAR(50),
    price DOUBLE,
    weight DOUBLE,
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(50),
    material VARCHAR(50),
    description VARCHAR(1024),
    rating DOUBLE,
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    created_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3_db_postgres',
    'table-name' = 'dim_product',
    'username' = 'user',
    'password' = 'user',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_store (
    store_id INT,
    name VARCHAR(100),
    location VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP(3),
    PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3_db_postgres',
    'table-name' = 'dim_store',
    'username' = 'user',
    'password' = 'user',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE dim_supplier (
    supplier_id INT,
    name VARCHAR(100),
    contact VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(150),
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP(3),
    PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3_db_postgres',
    'table-name' = 'dim_supplier',
    'username' = 'user',
    'password' = 'user',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE fact_sales (
    sales_id INT,
    sale_date DATE,
    customer_id INT,
    seller_id INT,
    product_id INT,
    store_id INT,
    supplier_id INT,
    quantity INT,
    total_price DOUBLE,
    created_at TIMESTAMP(3),
    PRIMARY KEY (sales_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3_db_postgres',
    'table-name' = 'fact_sales',
    'username' = 'user',
    'password' = 'user',
    'driver' = 'org.postgresql.Driver'
);


INSERT INTO dim_customer
SELECT DISTINCT
    sale_customer_id AS customer_id,
    customer_first_name AS first_name,
    customer_last_name AS last_name,
    customer_age AS age,
    customer_email AS email,
    customer_country AS country,
    customer_postal_code AS postal_code,
    customer_pet_type AS pet_type,
    customer_pet_name AS pet_name,
    customer_pet_breed AS pet_breed,
    pet_category,
    LOCALTIMESTAMP AS created_at
FROM kafka_source_events
WHERE sale_customer_id IS NOT NULL;

INSERT INTO dim_seller
SELECT DISTINCT
    sale_seller_id AS seller_id,
    seller_first_name AS first_name,
    seller_last_name AS last_name,
    seller_email AS email,
    seller_country AS country,
    seller_postal_code AS postal_code,
    LOCALTIMESTAMP AS created_at
FROM kafka_source_events
WHERE sale_seller_id IS NOT NULL;

INSERT INTO dim_product
SELECT DISTINCT
    sale_product_id AS product_id,
    product_name AS name,
    product_category AS category,
    CAST(product_price AS DOUBLE) AS price,
    CAST(product_weight AS DOUBLE) AS weight,
    product_color AS color,
    product_size AS size,
    product_brand AS brand,
    product_material AS material,
    product_description AS description,
    CAST(product_rating AS DOUBLE) AS rating,
    product_reviews AS reviews,
    TRY_CAST(SUBSTRING(product_release_date FROM 1 FOR 10) AS DATE) AS release_date,
    TRY_CAST(SUBSTRING(product_expiry_date FROM 1 FOR 10) AS DATE) AS expiry_date,
    LOCALTIMESTAMP AS created_at
FROM kafka_source_events
WHERE sale_product_id IS NOT NULL;

INSERT INTO dim_store
SELECT DISTINCT
    ABS(HASH_CODE(store_email)) % 2147483647 AS store_id,
    store_name AS name,
    store_location AS location,
    store_city AS city,
    store_state AS state,
    store_country AS country,
    store_phone AS phone,
    store_email AS email,
    LOCALTIMESTAMP AS created_at
FROM kafka_source_events
WHERE store_email IS NOT NULL;

INSERT INTO dim_supplier
SELECT DISTINCT
    ABS(HASH_CODE(supplier_name)) % 2147483647 AS supplier_id,
    supplier_name AS name,
    supplier_contact AS contact,
    supplier_email AS email,
    supplier_phone AS phone,
    supplier_address AS address,
    supplier_city AS city,
    supplier_country AS country,
    LOCALTIMESTAMP AS created_at
FROM kafka_source_events
WHERE supplier_name IS NOT NULL;

INSERT INTO fact_sales
SELECT
    k.id AS sales_id,
    TRY_CAST(SUBSTRING(k.sale_date FROM 1 FOR 10) AS DATE) AS sale_date,
    k.sale_customer_id AS customer_id,
    k.sale_seller_id AS seller_id,
    k.sale_product_id AS product_id,
    ABS(HASH_CODE(k.store_email)) % 2147483647 AS store_id,
    ABS(HASH_CODE(k.supplier_name)) % 2147483647 AS supplier_id,
    k.sale_quantity AS quantity,
    CAST(k.sale_total_price AS DOUBLE) AS total_price,
    LOCALTIMESTAMP AS created_at
FROM kafka_source_events k
WHERE k.sale_customer_id IS NOT NULL
  AND k.sale_seller_id IS NOT NULL
  AND k.sale_product_id IS NOT NULL
  AND k.store_email IS NOT NULL
  AND k.supplier_name IS NOT NULL;
