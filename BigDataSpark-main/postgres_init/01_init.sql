CREATE TABLE IF NOT EXISTS mock_data (
    id INT,
    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_age INT,
    customer_email VARCHAR(50),
    customer_country VARCHAR(50),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(50),
    customer_pet_breed VARCHAR(50),
    seller_first_name VARCHAR(50),
    seller_last_name VARCHAR(50),
    seller_email VARCHAR(50),
    seller_country VARCHAR(50),
    seller_postal_code VARCHAR(50),
    product_name VARCHAR(50),
    product_category VARCHAR(50),
    product_price REAL,
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price REAL,
    store_name VARCHAR(50),
    store_location VARCHAR(50),
    store_city VARCHAR(50),
    store_state VARCHAR(50),
    store_country VARCHAR(50),
    store_phone VARCHAR(50),
    store_email VARCHAR(50),
    pet_category VARCHAR(50),
    product_weight REAL,
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(50),
    product_material VARCHAR(50),
    product_description VARCHAR(1024),
    product_rating REAL,
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(50),
    supplier_contact VARCHAR(50),
    supplier_email VARCHAR(50),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(50),
    supplier_city VARCHAR(50),
    supplier_country VARCHAR(50)
);

COPY mock_data FROM '/data/MOCK_DATA.csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (1).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (2).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (3).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (4).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (5).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (6).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (7).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (8).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');
COPY mock_data FROM '/data/MOCK_DATA (9).csv' WITH (FORMAT CSV, HEADER true, NULL 'NULL');