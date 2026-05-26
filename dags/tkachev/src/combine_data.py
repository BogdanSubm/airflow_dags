from airflow.hooks.base import BaseHook

def combine_data (**context):
    import psycopg2 as pg

    sql_query = f"""
            INSERT INTO tkachev_agg_table
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1),
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
                   '{context['ds']}'::timestamp
            FROM tkachev_api_table
            WHERE created_at >= '{context['ds']}'::timestamp
              AND created_at < '{context['ds']}'::timestamp + INTERVAL '1 days'
            GROUP BY lti_user_id, attempt_type;
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname="etl",
        sslmode="disable",
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:

        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()