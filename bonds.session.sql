WITH coupon_payments AS (
    SELECT coupons.isin AS isin,
        sum(
            CASE
                WHEN (coupons.payday > date('now')) THEN (coupons.coup + coupons.amort) * power(
                    1 - 0.15 / 365,
                    julianday(coupons.payday) - julianday(date('now'))
                )
                ELSE 0
            END
        ) AS total_income_i,
        sum(
            CASE
                WHEN (coupons.payday > date('now')) THEN coupons.coup + coupons.amort
                ELSE 0
            END
        ) AS total_income
    FROM coupons
    where isin = 'RU000A0ZYHX8'
    GROUP BY coupons.isin
)
SELECT bonds.has_amort,
    bonds.isin AS isin,
    (
        julianday(coalesce(bonds.oferta, bonds.end_date)) - julianday(date('now'))
    ) / 365 AS years,
    (
        (bonds.offer / 100) * bonds.nominal + coalesce(bonds.nkd, 0)
    ) * 1.0006 AS purchase_price,
    CASE
        WHEN (bonds.has_amort = false) THEN coupon_payments.total_income + bonds.nominal * power(
            1 - 0.15 / 365,
            julianday(coalesce(bonds.oferta, bonds.end_date)) - julianday(date('now'))
        )
        ELSE coupon_payments.total_income_i
    END AS total_income_i,
    CASE
        WHEN (bonds.has_amort = false) THEN coupon_payments.total_income + bonds.nominal
        ELSE coupon_payments.total_income
    END AS total_income
FROM bonds
    JOIN coupon_payments ON bonds.isin = coupon_payments.isin
WHERE bonds.offer IS NOT NULL
    AND bonds.nominal IS NOT NULL
    AND coalesce(bonds.oferta, bonds.end_date) > date('now')
),
final_query AS (
    SELECT calc_query.isin AS isin,
        calc_query.purchase_price AS purchase_price,
        (
            calc_query.total_income / calc_query.purchase_price - 1
        ) * 100 AS yield_p,
        (
            calc_query.total_income_i / calc_query.purchase_price - 1
        ) * 100 AS yield_i,
        calc_query.years AS years
    FROM calc_query
        JOIN bonds ON bonds.isin = calc_query.isin
)
SELECT final_query.isin,
    final_query.purchase_price,
    final_query.yield_p,
    final_query.yield_i,
    final_query.years,
    final_query.yield_p / final_query.years AS yield_y,
    final_query.yield_i / final_query.years AS yield_iy
FROM final_query