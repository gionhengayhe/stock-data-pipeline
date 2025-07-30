-- \c datasource;
SELECT
    c.name,
    c.ticker,
    c.is_delisted,
    c.category,
    c.currency,
    c.location,
    e.name as exchange,
    r.region,
    i.industry,
    i.sector,
    s.sic_industry,
    s.sic_sector,
    c.updated_time
FROM
    companies c
LEFT JOIN exchanges e
	ON c.exchange_id = e.id
LEFT JOIN regions r
	ON e.region_id = r.id
LEFT JOIN industries i
	ON c.industry_id = i.id
LEFT JOIN sic_industries s
	ON c.sic_id = s.id
WHERE
    DATE_TRUNC('day', c.updated_time) = DATE_TRUNC('day', CURRENT_DATE)
    AND DATE_TRUNC('month', c.updated_time) = DATE_TRUNC('month', CURRENT_DATE)
    AND DATE_TRUNC('year', c.updated_time) = DATE_TRUNC('year', CURRENT_DATE)
