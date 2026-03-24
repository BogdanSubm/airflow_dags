# ================= DDL ===================

sql_creates_dict = {
    'rocknmove_raw_data': """
        CREATE TABLE IF NOT EXISTS {} (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
            , lti_user_id TEXT NOT NULL
            , oauth_consumer_key TEXT
            , lis_result_sourcedid TEXT
            , lis_outcome_service_url TEXT
            , is_correct INT CHECK (is_correct in (0, 1) OR is_correct is NULL)
            , attempt_type TEXT CHECK (attempt_type in ('run', 'submit'))
            , created_at TIMESTAMP
            , date_tag TEXT
            , UNIQUE (lti_user_id, created_at)
            ) """,

    'rocknmove_users': """
        CREATE TABLE IF NOT EXISTS {} (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
            , lti_user_id TEXT NOT NULL
            , first_activity TIMESTAMP NOT NULL
            , UNIQUE (lti_user_id)
            ) """,

    'rocknmove_data_agg1': """
        CREATE TABLE IF NOT EXISTS {} (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
            , period_start DATE
            , period_end DATE
            , attempts_total INTEGER
            , correct_attempts INTEGER
            , unique_users INTEGER
            , new_users INTEGER
            , date_tag TEXT
            , UNIQUE (period_start, period_end)
            )"""
}


# ================= DML and DQL ===================
sql_agg = """
    INSERT INTO roc_data_agg1 
    (period_start, period_end, attempts_total, correct_attempts, unique_users, new_users, date_tag) 
        SELECT
            %s::date AS period_start
            , (%s::date - INTERVAL '1 DAY')::date AS period_end
            , count(1) AS attempts_total
            , count(CASE WHEN d.is_correct=1 THEN 1 ELSE NULL END) AS correct_attempts
            , count(DISTINCT d.lti_user_id) AS unique_users
            , count(DISTINCT CASE WHEN u.lti_user_id IS NULL THEN d.lti_user_id ELSE NULL END) AS new_users
            , date_tag
        FROM roc_raw_data d
        LEFT JOIN (SELECT DISTINCT lti_user_id FROM roc_raw_data WHERE created_at < %s::date) u ON d.lti_user_id=u.lti_user_id
        WHERE created_at >= %s::date
            AND created_at < %s::date
        GROUP BY date_tag, period_start, period_end
    ON CONFLICT (period_start, period_end) DO NOTHING
"""

sql_add_users = """
    INSERT INTO roc_users 
    (lti_user_id, first_activity) 
        SELECT DISTINCT 
            lti_user_id
            , min(created_at) AS first_activity
        FROM roc_raw_data
        WHERE created_at >= %s::date
            AND created_at < %s::date
        GROUP BY lti_user_id
    ON CONFLICT (lti_user_id) DO NOTHING
"""
sql_check_table = """
    SELECT 1
    FROM {}
    LIMIT 1
"""
