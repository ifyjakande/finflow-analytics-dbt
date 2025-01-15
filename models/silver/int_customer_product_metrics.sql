WITH product_adoption AS (
    SELECT
        c.customer_key,
        -- Product diversity
        COUNT(DISTINCT CASE WHEN p.product_category IS NOT NULL 
              THEN p.product_category END) as product_categories_used,
        COUNT(DISTINCT CASE WHEN p.product_subcategory IS NOT NULL 
              THEN p.product_subcategory END) as product_subcategories_used,
        
        -- Product usage timeline
        MIN(a.opened_date) as first_product_date,
        MAX(a.opened_date) as latest_product_date,
        
        -- Active products
        COUNT(DISTINCT CASE 
            WHEN a.account_status = 'Active' AND p.product_key IS NOT NULL
            THEN p.product_key 
        END) as active_products_count
    FROM {{ ref('src_customers') }} c
    LEFT JOIN {{ ref('src_accounts') }} a 
        ON c.customer_key = a.customer_key
    LEFT JOIN {{ ref('src_products') }} p 
        ON a.product_key = p.product_key
    GROUP BY 1
)

SELECT * FROM product_adoption
