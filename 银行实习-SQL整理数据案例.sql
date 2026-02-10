-- 注明：再此展示的代码仅供展示个人SQL能力，不涉及任何敏感信息。所有数据与变量名称均已进行脱敏处理

-- 导入数据
WITH bureau_latest_pre_orig AS (
  SELECT
    l.loan_id,
    cb.customer_id,
    cb.credit_score,
    cb.total_utilization,
    cb.delinquencies_12m,
    cb.report_date,
    ROW_NUMBER() OVER (
      PARTITION BY l.loan_id
      ORDER BY cb.report_date DESC
    ) AS rn
  FROM loans l
  JOIN credit_bureau cb
    ON cb.customer_id = l.customer_id
   AND cb.report_date <= l.origination_date
)
SELECT *
FROM bureau_latest_pre_orig
WHERE rn = 1;

-- 生成和还款相关的变量(例如还款次数，已还款、未还款、逾期)
WITH pay_features AS (
  SELECT
    p.loan_id,
    COUNT(*) AS n_sched_payments,
    SUM(CASE WHEN p.amount_paid >= p.amount_due THEN 1 ELSE 0 END) AS n_on_time_or_full,
    SUM(CASE WHEN p.paid_date IS NULL THEN 1 ELSE 0 END) AS n_unpaid,
    MAX(
      CASE
        WHEN p.paid_date IS NULL THEN DATEDIFF(CURDATE(), p.due_date)
        ELSE DATEDIFF(p.paid_date, p.due_date)
      END
    ) AS max_days_past_due
  FROM payments p
  GROUP BY p.loan_id
)
SELECT * FROM pay_features;

-- 整合成可观的整体表格
WITH bureau_latest_pre_orig AS (
  SELECT
    l.loan_id,
    cb.credit_score,
    cb.total_utilization,
    cb.delinquencies_12m,
    ROW_NUMBER() OVER (PARTITION BY l.loan_id ORDER BY cb.report_date DESC) AS rn
  FROM loans l
  JOIN credit_bureau cb
    ON cb.customer_id = l.customer_id
   AND cb.report_date <= l.origination_date
),
income_latest AS (
  SELECT
    ci.customer_id,
    ci.monthly_income,
    ci.income_verified,
    ROW_NUMBER() OVER (PARTITION BY ci.customer_id ORDER BY ci.updated_at DESC) AS rn
  FROM customer_income ci
),
pay_features AS (
  SELECT
    p.loan_id,
    COUNT(*) AS n_sched_payments,
    SUM(CASE WHEN p.amount_paid >= p.amount_due THEN 1 ELSE 0 END) AS n_full_paid,
    SUM(CASE WHEN p.paid_date IS NULL THEN 1 ELSE 0 END) AS n_unpaid,
    MAX(
      CASE
        WHEN p.paid_date IS NULL THEN DATEDIFF(CURDATE(), p.due_date)
        ELSE DATEDIFF(p.paid_date, p.due_date)
      END
    ) AS max_days_past_due
  FROM payments p
  GROUP BY p.loan_id
),
target_default AS (
  SELECT
    l.loan_id,
    CASE
      WHEN l.status IN ('DEFAULT', 'CHARGED_OFF') THEN 1
      WHEN COALESCE(pf.max_days_past_due, 0) >= 90 THEN 1
      ELSE 0
    END AS default_flag
  FROM loans l
  LEFT JOIN pay_features pf ON pf.loan_id = l.loan_id
)
SELECT
  l.loan_id,
  l.customer_id,
  l.loan_type,
  l.principal,
  l.interest_rate,
  l.term_months,
  l.origination_date,

  c.gender,
  c.state,
  TIMESTAMPDIFF(YEAR, c.dob, l.origination_date) AS age_at_orig,

  il.monthly_income,
  il.income_verified,

  bl.credit_score,
  bl.total_utilization,
  bl.delinquencies_12m,

  pf.n_sched_payments,
  pf.n_full_paid,
  pf.n_unpaid,
  pf.max_days_past_due,

  td.default_flag
FROM loans l
JOIN customers c ON c.customer_id = l.customer_id
LEFT JOIN (SELECT * FROM bureau_latest_pre_orig WHERE rn = 1) bl ON bl.loan_id = l.loan_id
LEFT JOIN (SELECT * FROM income_latest WHERE rn = 1) il ON il.customer_id = l.customer_id
LEFT JOIN pay_features pf ON pf.loan_id = l.loan_id
JOIN target_default td ON td.loan_id = l.loan_id;

-- 数据探索：
-- 全局未能还款率：
SELECT
  COUNT(*) AS n_loans,
  SUM(default_flag) AS n_default,
  ROUND(AVG(default_flag) * 100, 2) AS default_rate_pct
FROM loan_model_snapshot;

-- 根据贷款种类分类的还款率
SELECT
  loan_type,
  COUNT(*) AS n,
  ROUND(AVG(default_flag) * 100, 2) AS default_rate_pct,
  ROUND(AVG(principal), 2) AS avg_principal,
  ROUND(AVG(interest_rate), 4) AS avg_rate
FROM loan_model_snapshot
GROUP BY loan_type
ORDER BY default_rate_pct DESC;

-- 查看数据分布 以及查看有无个例(outliers)
WITH ranked AS (
  SELECT
    principal,
    credit_score,
    monthly_income,
    NTILE(100) OVER (ORDER BY principal) AS principal_pctl,
    NTILE(100) OVER (ORDER BY monthly_income) AS income_pctl
  FROM loan_model_snapshot
)
SELECT
  MAX(CASE WHEN principal_pctl = 1 THEN principal END) AS principal_p01,
  MAX(CASE WHEN principal_pctl = 50 THEN principal END) AS principal_p50,
  MAX(CASE WHEN principal_pctl = 99 THEN principal END) AS principal_p99,
  MAX(CASE WHEN income_pctl = 1 THEN monthly_income END) AS income_p01,
  MAX(CASE WHEN income_pctl = 50 THEN monthly_income END) AS income_p50,
  MAX(CASE WHEN income_pctl = 99 THEN monthly_income END) AS income_p99
FROM ranked;


-- 