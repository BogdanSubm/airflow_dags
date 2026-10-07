Jconfig = [
    {
        "table_name": "juventa_table1",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS {{ table_name }} (                 
                attempt_type text,
                amount NUMERIC,
                date_start timestamp,
                date_end timestamp
            ); 
        """,
        "table_dml": """
            INSERT INTO {{ table_name }} (attempt_type, amount, date_start, date_end)    
            SELECT
                attempt_type,
                COUNT(1) AS amount,
                %s::timestamp as date_start,
                %s::timestamp as date_end
            FROM juventa_raw_table 
            WHERE created_at >= %s::timestamp
              AND created_at < %s::timestamp + interval '1 day'  
            GROUP BY attempt_type
        """,
        "need_to_export": False,
    },
    {
        "table_name": "juventa_table2",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS {{ table_name }} (
                action_amount int,
                users_unique int,                
                date_start timestamp,
                date_end timestamp
            ); 
        """,
        "table_dml": """
            INSERT INTO {{ table_name }} (action_amount, users_unique, date_start, date_end)    
            SELECT
                COUNT(1) as action_amount,
                COUNT(distinct lti_user_id) as users_unique,
                %s::timestamp as date_start,
                %s::timestamp as date_end
            FROM juventa_raw_table 
            WHERE created_at >= %s::timestamp
              AND created_at < %s::timestamp + interval '1 day'  

        """,
        "need_to_export": True,
    }
]