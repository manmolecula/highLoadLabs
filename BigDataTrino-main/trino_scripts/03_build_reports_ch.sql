-- ============== 1. Витрина продаж по продуктам ==============
-- Топ-10 самых продаваемых продуктов
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_top10_products AS
SELECT 
    fs.product_id,
    dp.name AS product_name,
    SUM(fs.quantity) AS total_qty,
    SUM(fs.total_price) AS revenue
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_product dp ON fs.product_id = dp.product_id
GROUP BY fs.product_id, dp.name
ORDER BY total_qty DESC
LIMIT 10;


-- Общая выручка по категориям продуктов
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_revenue_by_category AS
SELECT 
    dp.category,
    SUM(fs.total_price) AS total_revenue,
    SUM(fs.quantity) AS total_quantity,
    AVG(fs.total_price / NULLIF(fs.quantity, 0)) AS avg_price
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.category
ORDER BY total_revenue DESC;


-- Средний рейтинг и количество отзывов для каждого продукта
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_product_avg_rating_reviews AS
SELECT 
    dp.product_id,
    dp.name AS product_name,
    AVG(dp.rating) AS avg_rating,
    SUM(dp.reviews) AS total_reviews
FROM clickhouse.default.dim_product dp
GROUP BY dp.product_id, dp.name
ORDER BY avg_rating DESC, total_reviews DESC;


-- ============== 2. Витрина продаж по клиентам ==============

-- Топ-10 клиентов с наибольшей общей суммой покупок
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_top10_customers AS
SELECT 
    fs.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.country,
    SUM(fs.total_price) AS total_spent
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY fs.customer_id, dc.first_name, dc.last_name, dc.email, dc.country
ORDER BY total_spent DESC
LIMIT 10;


--Распределение клиентов по странам
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_customers_by_country AS
SELECT 
    dc.country,
    COUNT(DISTINCT fs.customer_id) AS customer_count,
    SUM(fs.total_price) AS total_revenue,
    AVG(fs.total_price) AS avg_revenue_per_customer
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.country
ORDER BY customer_count DESC;


-- Средний чек для каждого клиента
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_avg_check_per_customer AS
SELECT 
    fs.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.country,
    COUNT(*) AS transaction_count,
    AVG(fs.total_price) AS avg_check,
    SUM(fs.total_price) AS total_spent,
    AVG(fs.quantity) AS avg_quantity_per_transaction
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY fs.customer_id, dc.first_name, dc.last_name, dc.email, dc.country
ORDER BY avg_check DESC;


-- ============== 3. Витрина продаж по времени ==============

-- Месячный тренд
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_monthly_trend AS
SELECT 
    date_trunc('month', fs.sale_date) AS month,
    SUM(fs.total_price) AS revenue,
    SUM(fs.quantity) AS qty
FROM clickhouse.default.fact_sales fs
GROUP BY date_trunc('month', fs.sale_date)
ORDER BY month;


-- Годовой тренд
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_yearly_trend AS
SELECT 
    year(fs.sale_date) AS year,
    SUM(fs.total_price) AS revenue,
    SUM(fs.quantity) AS qty
FROM clickhouse.default.fact_sales fs
GROUP BY year(fs.sale_date)
ORDER BY year;


-- Сравнение выручки за разные периоды (текущий месяц и месяц прошлого года)
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_revenue_year_month_comparison AS
WITH sales_with_periods AS (
    SELECT
        year(fs.sale_date) AS year,
        month(fs.sale_date) AS month,
        date_format(fs.sale_date, 'yyyy-MM') AS month_str,
        SUM(fs.total_price) AS revenue
    FROM clickhouse.default.fact_sales fs
    GROUP BY year(fs.sale_date), month(fs.sale_date), date_format(fs.sale_date, 'yyyy-MM')
)
SELECT
    cy.year AS current_year,
    cy.month AS current_month,
    cy.month_str AS current_month_str,
    cy.revenue AS current_revenue,
    py.year AS previous_year,
    py.month AS previous_month,
    py.month_str AS previous_month_str,
    py.revenue AS previous_revenue,
    COALESCE(cy.revenue, 0) - COALESCE(py.revenue, 0) AS revenue_difference,
    CASE 
        WHEN py.revenue IS NULL OR py.revenue = 0 THEN NULL
        ELSE ROUND(((COALESCE(cy.revenue, 0) - COALESCE(py.revenue, 0)) / py.revenue) * 100, 2)
    END AS revenue_growth_percent
FROM sales_with_periods cy
LEFT JOIN sales_with_periods py 
    ON cy.month = py.month 
    AND cy.year = py.year + 1
ORDER BY cy.year DESC, cy.month DESC;


-- Средний размер заказа по месяцам
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_avg_order_month AS
SELECT 
    date_trunc('month', fs.sale_date) AS month,
    SUM(fs.total_price) AS total_revenue,
    COUNT(*) AS orders_count,
    ROUND(SUM(fs.total_price) / NULLIF(COUNT(*), 0), 2) AS avg_order_value
FROM clickhouse.default.fact_sales fs
GROUP BY date_trunc('month', fs.sale_date), date_format(fs.sale_date, 'yyyy-MM')
ORDER BY month DESC;


-- ============== 4. Витрина продаж по магазинам ==============

-- Топ-5 магазинов с наибольшей выручкой
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_top5_stores AS
SELECT 
    fs.store_name,
    ds.location,
    ds.city,
    ds.country,
    SUM(fs.total_price) AS revenue
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_store ds ON fs.store_name = ds.name
GROUP BY fs.store_name, ds.location, ds.city, ds.country
ORDER BY revenue DESC
LIMIT 5;


-- Распределение продаж по городам и странам
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_sales_by_city_country AS
SELECT 
    ds.city,
    ds.country,
    SUM(fs.total_price) AS total_revenue,
    SUM(fs.quantity) AS total_quantity,
    COUNT(*) AS transaction_count
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_store ds ON fs.store_name = ds.name
GROUP BY ds.city, ds.country
ORDER BY total_revenue DESC;


-- Средний чек для каждого магазина
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_avg_check_store AS
SELECT 
    fs.store_name,
    ds.location,
    ds.city,
    ds.country,
    SUM(fs.total_price) AS total_revenue,
    COUNT(*) AS orders_count,
    ROUND(SUM(fs.total_price) / NULLIF(COUNT(*), 0), 2) AS avg_check
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_store ds ON fs.store_name = ds.name
GROUP BY fs.store_name, ds.location, ds.city, ds.country
ORDER BY avg_check DESC;


-- ============== 5. Витрина продаж по поставщикам ==============

-- Топ-5 поставщиков с наибольшей выручкой
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_top5_suppliers AS
SELECT 
    fs.supplier_name,
    ds.country,
    SUM(fs.total_price) AS revenue
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_supplier ds ON fs.supplier_name = ds.name
GROUP BY fs.supplier_name, ds.country
ORDER BY revenue DESC
LIMIT 5;


-- Средняя цена товаров от каждого поставщика
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_avg_price_supplier AS
SELECT 
    fs.supplier_name,
    ds.country,
    AVG(fs.total_price / NULLIF(fs.quantity, 0)) AS avg_price_per_unit,
    SUM(fs.total_price) AS total_revenue,
    SUM(fs.quantity) AS total_quantity
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_supplier ds ON fs.supplier_name = ds.name
GROUP BY fs.supplier_name, ds.country
ORDER BY avg_price_per_unit DESC;


-- Распределение продаж по странам поставщиков
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_sales_by_supplier_country AS
SELECT 
    ds.country,
    SUM(fs.total_price) AS revenue,
    SUM(fs.quantity) AS total_quantity,
    COUNT(*) AS transaction_count
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_supplier ds ON fs.supplier_name = ds.name
GROUP BY ds.country
ORDER BY revenue DESC;


-- ============== 6. Витрина качества продукции ==============

-- Продукты с наивысшим рейтингом
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_product_ratings_highest AS
SELECT 
    dp.product_id,
    dp.name AS product_name,
    dp.rating,
    dp.reviews
FROM clickhouse.default.dim_product dp
WHERE dp.rating IS NOT NULL
ORDER BY dp.rating DESC
LIMIT 10;


-- Продукты с наименьшим рейтингом
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_product_ratings_lowest AS
SELECT 
    dp.product_id,
    dp.name AS product_name,
    dp.rating,
    dp.reviews
FROM clickhouse.default.dim_product dp
WHERE dp.rating IS NOT NULL
ORDER BY dp.rating ASC
LIMIT 10;


-- Корреляция между рейтингом и объемом продаж
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_rating_sales_corr AS
SELECT 
    fs.product_id,
    dp.name AS product_name,
    dp.rating,
    SUM(fs.quantity) AS total_qty,
    SUM(fs.total_price) AS total_revenue
FROM clickhouse.default.fact_sales fs
JOIN clickhouse.default.dim_product dp ON fs.product_id = dp.product_id
GROUP BY fs.product_id, dp.name, dp.rating
ORDER BY dp.rating DESC, total_qty DESC;


-- Продукты с наибольшим количеством отзывов
CREATE TABLE IF NOT EXISTS clickhouse.default.mart_most_reviews AS
SELECT 
    dp.product_id,
    dp.name AS product_name,
    dp.reviews,
    dp.rating
FROM clickhouse.default.dim_product dp
WHERE dp.reviews IS NOT NULL
ORDER BY dp.reviews DESC, dp.rating DESC
LIMIT 100;