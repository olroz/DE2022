DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(purchase_date) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(CustomerId AS INTEGER) AS client_id,
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate), 
        SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate), 
        SAFE.PARSE_DATE('%Y.%m.%d', PurchaseDate)
    ) AS purchase_date,
    Product,
    CAST(RTRIM(Price, '$') AS INTEGER) AS price,

    _id,
    _logical_dt,
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP) AS _job_start_dt
FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = "{{ ds }}"
;
