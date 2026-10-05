-- Project 1, Group 17: demo queries in pseudocode
-- Sector of a fact = sector of the DimCompany row the fact points to.

-- Q1. Which sectors depend the most on public procurement? (KPI 1)
procurement = SELECT sector, year, SUM(awarded_contract_amount)
              FROM FactProcurement JOIN DimCompany JOIN DimSector JOIN DimDate
              GROUP BY sector, year
turnover    = SELECT sector, year, SUM(revenue)
              FROM FactCompany JOIN DimCompany JOIN DimSector JOIN DimDate
              GROUP BY sector, year
SELECT sector, year, procurement / turnover * 100 AS dependency_rate_pct
FROM turnover JOIN procurement ON sector, year
ORDER BY dependency_rate_pct DESC

-- Q2. Where is turnover rising while employees fall, and where is it the opposite? (KPI 2)
SELECT sector, year, quarter,
       SUM(revenue)        vs. previous quarter AS revenue_change,
       SUM(employee_count) vs. previous quarter AS employee_change,
       CASE WHEN revenue_change > 0 AND employee_change < 0 THEN 'efficiency gain'
            WHEN revenue_change < 0 AND employee_change > 0 THEN 'efficiency loss'
       END AS efficiency_signal
FROM FactCompany JOIN DimCompany JOIN DimSector JOIN DimDate
GROUP BY sector, year, quarter

-- Q3. Are procurements won by companies with no employees or no prior activity? (KPI 3)
FOR EACH contract IN FactProcurement:
    employees_at_award = winner's' employee_count in FactCompany in the award quarter
    prior_activity     = winner has a quarter with revenue > 0 before the award
    is_anomalous       = employees_at_award = 0 OR NOT prior_activity
SELECT sector,
       COUNT(contracts WHERE is_anomalous),
       SUM(amount WHERE is_anomalous) / SUM(amount) * 100 AS anomalous_win_rate_pct
GROUP BY sector

-- Q4. In which sectors, and to what extent, are won procurements EU co-funded?
SELECT sector,
       COUNT(contracts),
       COUNT(contracts WHERE eu_funded),
       SUM(amount WHERE eu_funded) / SUM(amount) * 100 AS eu_funded_share_pct
FROM FactProcurement JOIN DimProcurement JOIN DimCompany JOIN DimSector
GROUP BY sector

-- Q5. Are recently established companies able to win procurements, and in which sectors?
SELECT sector,
       COUNT(contracts WHERE signing_date < registration_date + 12 months) AS won_by_new_companies,
       SUM(amount      WHERE signing_date < registration_date + 12 months) AS amount_won_by_new_companies
FROM FactProcurement JOIN DimCompany JOIN DimSector JOIN DimDate
GROUP BY sector
