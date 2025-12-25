INSERT INTO dim_customer (
    first_name,
    last_name,
    age,
    email,
    country,
    postal_code,
    pet_type,
    pet_name,
    pet_breed,
    pet_category
)
SELECT DISTINCT
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
FROM mock_data;


INSERT INTO dim_seller (
    first_name,
    last_name,
    email,
    country,
    postal_code
)
SELECT DISTINCT
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data;


INSERT INTO dim_product (
    name,
    category,
    price,
    weight,
    color,
    size,
    brand,
    material,
    description,
    rating,
    reviews,
    release_date,
    expiry_date
)
SELECT DISTINCT
    product_name,
    product_category,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
FROM mock_data;


INSERT INTO dim_store (
    name,
    location,
    city,
    state,
    country,
    phone,
    email
)
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data;


INSERT INTO dim_supplier (
    name,
    contact,
    email,
    phone,
    address,
    city,
    country
)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data;


INSERT INTO fact_sales (
    sale_date,
    customer_id,
    seller_id,
    product_id,
    store_id,
    supplier_id,
    quantity,
    total_price
)
SELECT
    m.sale_date,
    dc.customer_id,
    ds.seller_id,
    dp.product_id,
    dst.store_id,
    dsu.supplier_id,
    m.sale_quantity,
    m.sale_total_price
FROM mock_data m
JOIN dim_customer dc
    ON dc.first_name = m.customer_first_name 
	AND dc.last_name  = m.customer_last_name
    AND dc.email = m.customer_email
JOIN dim_seller ds
    ON ds.first_name = m.seller_first_name
    AND ds.last_name = m.seller_last_name
    AND ds.email = m.seller_email
JOIN dim_product dp
    ON dp.name = m.product_name
    AND dp.category = m.product_category
    AND dp.price = m.product_price
JOIN dim_store dst
    ON dst.name = m.store_name
    AND dst.city = m.store_city
    AND dst.country = m.store_country
JOIN dim_supplier dsu
	ON dsu.name = m.supplier_name
    AND dsu.email = m.supplier_email;


DROP TABLE IF EXISTS mock_data;