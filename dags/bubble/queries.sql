CREATE TABLE IF NOT EXISTS soldatowiw_raw_table (
    lti_user_id             TEXT,
    is_correct              BOOLEAN,
    attempt_type            TEXT,
    created_at              TIMESTAMP,
    oauth_consumer_key      TEXT,
    lis_result_sourcedid    TEXT,
    lis_outcome_service_url TEXT,
    ds                      DATE 
);

CREATE TABLE IF NOT EXISTS soldatowiw_agg_table (
      period_start        DATE        NOT NULL,
      period_end          DATE        NOT NULL,
      total_attempts      INTEGER     NOT NULL,
      correct_attempts    INTEGER     NOT NULL,
      incorrect_attempts  INTEGER     NOT NULL,
      unique_users        INTEGER     NOT NULL,
      correct_rate        NUMERIC(5, 4) NOT NULL,
      PRIMARY KEY (period_start, period_end)
  );