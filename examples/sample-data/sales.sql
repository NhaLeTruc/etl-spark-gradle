-- Sample data setup for Quickstart Scenario 1
-- PostgreSQL sales database

CREATE TABLE sales (
  sale_id SERIAL PRIMARY KEY,
  product_id VARCHAR(50) NOT NULL,
  category VARCHAR(50) NOT NULL,
  quantity INT NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  sale_date DATE NOT NULL
);

-- Insert sample sales data (1000 records)
INSERT INTO sales (product_id, category, quantity, price, sale_date)
SELECT
  'PROD-' || ((n % 100) + 1),
  CASE
    WHEN n % 3 = 0 THEN 'Electronics'
    WHEN n % 3 = 1 THEN 'Clothing'
    ELSE 'Books'
  END,
  (n % 10) + 1,
  10.0 + (n % 500) / 10.0,
  DATE '2024-01-01' + (n % 365)
FROM generate_series(1, 1000) AS n;

-- Create indexes for better query performance
CREATE INDEX idx_sales_category ON sales(category);
CREATE INDEX idx_sales_date ON sales(sale_date);
CREATE INDEX idx_sales_product ON sales(product_id);

-- Verify data
SELECT
  category,
  COUNT(*) as sale_count,
  SUM(quantity) as total_quantity,
  SUM(price * quantity) as total_revenue,
  AVG(price) as avg_price
FROM sales
GROUP BY category
ORDER BY category;
