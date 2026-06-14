# Для выполнения задания создал таблицу-пример с фактами покупок подписки СберПрайм.
# sberprime_subscription_purchases
# ------------------------------------------------------------
# purchase_id         BIGINT PRIMARY KEY          -- уникальный ID покупки
# client_id           BIGINT NOT NULL             -- ID клиента (пользователя)
# purchase_date       DATE NOT NULL               -- дата покупки
# subscription_plan   VARCHAR(20) NOT NULL        -- тип подписки: 'month', 'year', 'trial'
# price               DECIMAL(10,2) NOT NULL      -- стоимость в рублях
# payment_method      VARCHAR(30)                 -- способ оплаты: 'card', 'sberpay', 'bonus'
# region              VARCHAR(50)                 -- регион клиента (RU-MOW, RU-SPB и т.д.)
# discount_applied    BOOLEAN DEFAULT FALSE       -- применялась ли скидка
# status              VARCHAR(20) DEFAULT 'success'  -- 'success', 'failed', 'refund'

config = [
    {
        "table_name": "agg_sberprime_daily_revenue",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS agg_sberprime_daily_revenue (
                purchase_date DATE PRIMARY KEY,
                total_revenue DECIMAL(15,2),
                revenue_monthly_plan DECIMAL(15,2),
                revenue_yearly_plan DECIMAL(15,2),
                revenue_trial DECIMAL(15,2),
                total_purchases INTEGER,
                loaded_at TIMESTAMP DEFAULT NOW()
            );
        """,
        "table_dml": """
            DELETE FROM agg_sberprime_daily_revenue WHERE purchase_date = '{{ ds }}';
            INSERT INTO agg_sberprime_daily_revenue
            (purchase_date, total_revenue, revenue_monthly_plan, revenue_yearly_plan, revenue_trial, total_purchases)
            SELECT
                '{{ ds }}' AS purchase_date,
                SUM(price) AS total_revenue,
                SUM(CASE WHEN subscription_plan = 'month' THEN price ELSE 0 END) AS revenue_monthly_plan,
                SUM(CASE WHEN subscription_plan = 'year' THEN price ELSE 0 END) AS revenue_yearly_plan,
                SUM(CASE WHEN subscription_plan = 'trial' THEN price ELSE 0 END) AS revenue_trial,
                COUNT(*) AS total_purchases
            FROM sberprime_subscription_purchases
            WHERE purchase_date = '{{ ds }}' AND status = 'success';
        """,
        "need_to_export": True
    },
    {
        "table_name": "agg_sberprime_payment_method",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS agg_sberprime_payment_method (
                purchase_date DATE,
                payment_method VARCHAR(30),
                total_purchases INTEGER,
                total_amount DECIMAL(15,2),
                PRIMARY KEY (purchase_date, payment_method)
            );
        """,
        "table_dml": """
            DELETE FROM agg_sberprime_payment_method WHERE purchase_date = '{{ ds }}';
            INSERT INTO agg_sberprime_payment_method
            (purchase_date, payment_method, total_purchases, total_amount)
            SELECT
                '{{ ds }}',
                payment_method,
                COUNT(*),
                SUM(price)
            FROM sberprime_subscription_purchases
            WHERE purchase_date = '{{ ds }}' AND status = 'success'
            GROUP BY purchase_date, payment_method;
        """,
        "need_to_export": False
    }
]