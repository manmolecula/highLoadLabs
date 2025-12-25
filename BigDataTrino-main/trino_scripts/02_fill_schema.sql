USE clickhouse.mock_data;

-- CUSTOMERS
INSERT INTO clickhouse.default.dim_customer
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
    pet_category AS pet_category,
    now() AS created_at
FROM (
    SELECT 
        sale_customer_id, 
        customer_first_name, 
        customer_last_name, 
        customer_age, 
        customer_email, 
        customer_country, 
        customer_postal_code, 
        customer_pet_type, 
        customer_pet_name, 
        customer_pet_breed,
        pet_category
    FROM postgresql.public.mock_data
    WHERE sale_customer_id IS NOT NULL
    UNION ALL
        SELECT 
        sale_customer_id, 
        customer_first_name, 
        customer_last_name, 
        customer_age, 
        customer_email, 
        customer_country, 
        customer_postal_code, 
        customer_pet_type, 
        customer_pet_name, 
        customer_pet_breed,
        pet_category
    FROM clickhouse.mock_data.mock_data
);


-- SELLERS
INSERT INTO clickhouse.default.dim_seller
SELECT DISTINCT
    sale_seller_id AS seller_id,
    seller_first_name AS first_name,
    seller_last_name AS last_name,
    seller_email AS email,
    seller_country AS country,
    seller_postal_code AS postal_code,
    now() AS created_at
FROM (
    SELECT 
        sale_seller_id, 
        seller_first_name, 
        seller_last_name, 
        seller_email, 
        seller_country, 
        seller_postal_code
    FROM postgresql.public.mock_data
    WHERE sale_seller_id IS NOT NULL
    
    UNION ALL

    SELECT 
        sale_seller_id, 
        seller_first_name, 
        seller_last_name, 
        seller_email, 
        seller_country, 
        seller_postal_code
    FROM clickhouse.mock_data.mock_data
    WHERE sale_seller_id IS NOT NULL
);


-- PRODUCTS
INSERT INTO clickhouse.default.dim_product
SELECT DISTINCT
    sale_product_id AS product_id,
    product_name AS name,
    product_category AS category,
    product_price AS price,
    product_weight AS weight,
    product_color AS color,
    product_size AS size,
    product_brand AS brand,
    product_material AS material,
    product_description AS description,
    product_rating AS rating,
    product_reviews AS reviews,
    product_release_date AS release_date,
    product_expiry_date AS expiry_date,
    CAST(now() AS DATE) AS created_at
FROM (
    SELECT
        sale_product_id,
        product_name,
        product_category,
        CAST(product_price AS DECIMAL(10, 2)) AS product_price,
        CAST(product_weight AS DECIMAL(10, 2)) AS product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        CAST(product_rating AS DECIMAL(3, 1)) AS product_rating,
        CAST(product_reviews AS INTEGER) AS product_reviews,
        CAST(product_release_date AS DATE) AS product_release_date,
        CAST(product_expiry_date AS DATE) AS product_expiry_date
    FROM postgresql.public.mock_data
    WHERE sale_product_id IS NOT NULL


    UNION ALL

    SELECT
        sale_product_id,
        product_name,
        product_category,
        CAST(product_price AS DECIMAL(10, 2)) AS product_price,
        CAST(product_weight AS DECIMAL(10, 2)) AS product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        CAST(product_rating AS DECIMAL(3, 1)) AS product_rating,
        CAST(product_reviews AS INTEGER) AS product_reviews,
        CAST(date_parse(product_release_date, '%m/%d/%Y') AS DATE) AS product_release_date,
        CAST(date_parse(product_expiry_date, '%m/%d/%Y') AS DATE) AS product_expiry_date
    FROM clickhouse.mock_data.mock_data
    WHERE sale_product_id IS NOT NULL
)
WHERE sale_product_id IS NOT NULL;


-- STORES
INSERT INTO clickhouse.default.dim_store
SELECT DISTINCT
    store_name AS name,
    store_location AS location,
    store_city AS city,
    store_state AS state,
    store_country AS country,
    store_phone AS phone,
    store_email AS email,
    CAST(now() AS DATE) AS created_at
FROM (
    SELECT
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email
    FROM postgresql.public.mock_data
    WHERE store_name IS NOT NULL

    UNION ALL

    SELECT
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email
    FROM clickhouse.mock_data.mock_data
    WHERE store_name IS NOT NULL
)
WHERE store_name IS NOT NULL;


-- SUPPLIERS
INSERT INTO clickhouse.default.dim_supplier
SELECT DISTINCT
    supplier_name AS name,
    supplier_contact AS contact,
    supplier_email AS email,
    supplier_phone AS phone,
    supplier_address AS address,
    supplier_city AS city,
    supplier_country AS country,
    now() AS created_at
FROM (
    SELECT 
        supplier_name, 
        supplier_contact, 
        supplier_email, 
        supplier_phone, 
        supplier_address, 
        supplier_city, 
        supplier_country
    FROM postgresql.public.mock_data
    WHERE supplier_name IS NOT NULL
    
    UNION ALL
    
    SELECT 
        supplier_name, 
        supplier_contact, 
        supplier_email, 
        supplier_phone, 
        supplier_address, 
        supplier_city, 
        supplier_country
    FROM clickhouse.mock_data.mock_data
    WHERE supplier_name IS NOT NULL
)
WHERE supplier_name IS NOT NULL;


-- SALES
INSERT INTO clickhouse.default.fact_sales
SELECT
    sale_date,
    sale_customer_id AS customer_id,
    sale_seller_id AS seller_id,
    sale_product_id AS product_id,
    store_name,
    supplier_name,
    quantity,
    total_price,
    CAST(now() AS DATE) AS created_at
FROM (
    SELECT 
        sale_customer_id, 
        sale_seller_id, 
        sale_product_id, 
        store_name,
        supplier_name,
        CAST(sale_date AS DATE) AS sale_date, 
        CAST(sale_quantity AS INTEGER) AS quantity, 
        CAST(sale_total_price AS DECIMAL(10, 2)) AS total_price
    FROM postgresql.public.mock_data
    WHERE sale_customer_id IS NOT NULL
      AND sale_seller_id IS NOT NULL
      AND sale_product_id IS NOT NULL
      AND sale_date IS NOT NULL
    
    UNION ALL
    
    SELECT 
        sale_customer_id, 
        sale_seller_id, 
        sale_product_id, 
        store_name,
        supplier_name,
        CAST(date_parse(sale_date, '%m/%d/%Y') AS DATE) AS sale_date, 
        CAST(sale_quantity AS INTEGER) AS quantity, 
        CAST(sale_total_price AS DECIMAL(10, 2)) AS total_price
    FROM clickhouse.mock_data.mock_data
    WHERE sale_customer_id IS NOT NULL
      AND sale_seller_id IS NOT NULL
      AND sale_product_id IS NOT NULL
      AND sale_date IS NOT NULL
)
WHERE sale_customer_id IS NOT NULL
  AND sale_seller_id IS NOT NULL
  AND sale_product_id IS NOT NULL
  AND sale_date IS NOT NULL;

