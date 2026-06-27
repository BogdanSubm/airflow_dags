# Сюда можно добавлять новые таблицы  без правки DAG
config = [
    {
        "table_name": "test_gd_agg",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS public.test_gd_agg (
                lti_user_id varchar(1000),
                total_rows bigint
            )
        """,
        "table_dml": """
            SELECT
                lti_user_id,
                COUNT(*) AS total_rows
            FROM public.test_gd
            GROUP BY lti_user_id
        """,
        "need_to_export": True,
        "export_columns": ["lti_user_id", "total_rows"],
    },
    {
        "table_name": "test_gd_agg2",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS public.test_gd_agg2 (
                lis_result_sourcedid varchar(1000),
                total_rows bigint
            )
        """,
        "table_dml": """
            SELECT
                lis_result_sourcedid,
                COUNT(*) AS total_rows
            FROM public.test_gd
            GROUP BY lis_result_sourcedid
        """,
        "need_to_export": True,
        "export_columns": ["lis_result_sourcedid", "total_rows"],
    },
    {
        "table_name": "test_gd_agg3",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS public.test_gd_agg3 (
                lis_outcome_service_url varchar(1000),
                total_rows bigint
            )
        """,
        "table_dml": """
            SELECT
                lis_outcome_service_url,
                COUNT(*) AS total_rows
            FROM public.test_gd
            GROUP BY lis_outcome_service_url
        """,
        "need_to_export": True,
        "export_columns": [ "total_rows"],
    }
    
]