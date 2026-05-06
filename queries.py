# ---------------------------------------------------------------------------
# SQL Queries
# Each query answers one of the 10 analytical questions from the design doc.
# ---------------------------------------------------------------------------

# Q1: What is the league-wide home vs. away win percentage by season?
Q1_BASELINE_WIN_PCT = """
SELECT
    ds.season_start_year,
    ds.season_id,
    ds.era,
    fts.location,
    ROUND(AVG(fts.w_pct), 4) AS avg_w_pct
FROM fact_team_season fts
JOIN dim_season ds ON fts.season_id = ds.season_id
GROUP BY ds.season_start_year, ds.season_id, ds.era, fts.location
ORDER BY ds.season_start_year, fts.location
"""

# Q2: How has the home court advantage gap changed year over year?
Q2_HCA_TREND = """
WITH season_gap AS (
    SELECT
        ds.season_start_year,
        ds.season_id,
        ds.era,
        AVG(CASE WHEN fts.location = 'Home' THEN fts.w_pct END) -
        AVG(CASE WHEN fts.location = 'Road' THEN fts.w_pct END) AS hca_gap
    FROM fact_team_season fts
    JOIN dim_season ds ON fts.season_id = ds.season_id
    GROUP BY ds.season_start_year, ds.season_id, ds.era
)
SELECT
    season_start_year,
    season_id,
    era,
    ROUND(hca_gap, 4) AS hca_gap,
    ROUND(
        hca_gap - LAG(hca_gap, 1) OVER (ORDER BY season_start_year),
        4
    ) AS yoy_change
FROM season_gap
ORDER BY season_start_year
"""

# Q3: How did home performance differ during the COVID bubble vs. normal seasons?
Q3_COVID_BUBBLE = """
SELECT
    ds.era,
    dg.is_bubble,
    ftg.is_home,
    ROUND(AVG(CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END), 4) AS w_pct,
    ROUND(AVG(ftg.plus_minus), 2) AS avg_plus_minus,
    COUNT(*) AS games
FROM fact_team_game ftg
JOIN dim_season ds ON ftg.season_id = ds.season_id
JOIN dim_game   dg ON ftg.game_id   = dg.game_id
GROUP BY ds.era, dg.is_bubble, ftg.is_home
ORDER BY ds.era, dg.is_bubble, ftg.is_home
"""

# Q4: Does fatigue (back-to-back) override home court advantage?
Q4_BACK_TO_BACK = """
SELECT
    CASE
        WHEN is_back_to_back = TRUE  THEN 'Back-to-Back'
        WHEN rest_days >= 3 THEN 'Well Rested'
        ELSE 'Normal Rest'
    END AS rest_bucket,
    is_home,
    ROUND(AVG(CASE WHEN wl = 'W' THEN 1.0 ELSE 0.0 END), 4) AS w_pct,
    COUNT(*) AS games
FROM fact_team_game
WHERE rest_days IS NOT NULL
GROUP BY rest_bucket, is_home
ORDER BY rest_bucket, is_home
"""

# Q5: Which teams get the biggest home shooting boost (FG% and FG3%)?
Q5_SHOOTING_BOOST = """
WITH team_shooting AS (
    SELECT
        ftg.team_id,
        AVG(CASE WHEN ftg.is_home = TRUE  THEN ftg.fg_pct  END) AS home_fg_pct,
        AVG(CASE WHEN ftg.is_home = FALSE THEN ftg.fg_pct  END) AS road_fg_pct,
        AVG(CASE WHEN ftg.is_home = TRUE  THEN ftg.fg3_pct END) AS home_fg3_pct,
        AVG(CASE WHEN ftg.is_home = FALSE THEN ftg.fg3_pct END) AS road_fg3_pct
    FROM fact_team_game ftg
    GROUP BY ftg.team_id
)
SELECT
    dt.full_name,
    dt.conference,
    ROUND(ts.home_fg_pct  - ts.road_fg_pct,  4) AS fg_pct_boost,
    ROUND(ts.home_fg3_pct - ts.road_fg3_pct, 4) AS fg3_pct_boost,
    RANK() OVER (ORDER BY (ts.home_fg3_pct - ts.road_fg3_pct) DESC) AS fg3_boost_rank
FROM team_shooting ts
JOIN dim_team dt ON ts.team_id = dt.team_id
ORDER BY fg3_boost_rank
"""

# Q6: What does a team's rolling 10-game home win percentage look like across a season?
Q6_ROLLING_HOME_WIN_PCT = """
SELECT
    ftg.team_id,
    dt.full_name,
    ftg.season_id,
    dg.game_date,
    dg.month,
    ROUND(
        AVG(CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END)
            OVER (
                PARTITION BY ftg.team_id, ftg.season_id
                ORDER BY dg.game_date
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ),
        4
    ) AS rolling_10_home_w_pct
FROM fact_team_game ftg
JOIN dim_game dg ON ftg.game_id  = dg.game_id
JOIN dim_team dt ON ftg.team_id  = dt.team_id
WHERE ftg.is_home = TRUE
ORDER BY ftg.team_id, ftg.season_id, dg.game_date
"""

# Q7: Do home teams consistently get more free throw attempts, and has the gap changed?
Q7_REFEREE_BIAS = """
WITH season_fta AS (
    SELECT
        ftg.season_id,
        AVG(CASE WHEN ftg.is_home = TRUE  THEN ftg.fta END) AS home_fta,
        AVG(CASE WHEN ftg.is_home = FALSE THEN ftg.fta END) AS road_fta
    FROM fact_team_game ftg
    GROUP BY ftg.season_id
)
SELECT
    sf.season_id,
    ds.season_start_year,
    ds.era,
    ROUND(sf.home_fta, 2)                                             AS home_fta,
    ROUND(sf.road_fta, 2)                                             AS road_fta,
    ROUND(sf.home_fta - sf.road_fta, 2)                               AS fta_gap,
    ROUND(
        (sf.home_fta - sf.road_fta) -
        LAG(sf.home_fta - sf.road_fta, 1) OVER (ORDER BY ds.season_start_year),
        2
    ) AS yoy_gap_change
FROM season_fta sf
JOIN dim_season ds ON sf.season_id = ds.season_id
ORDER BY ds.season_start_year
"""

# Q8: Is home court advantage stronger in the West than the East?
Q8_CONFERENCE_HCA = """
SELECT
    ds.season_start_year,
    ds.season_id,
    dt.conference,
    ROUND(
        AVG(CASE WHEN ftg.is_home = TRUE  THEN CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END END) -
        AVG(CASE WHEN ftg.is_home = FALSE THEN CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END END),
        4
    ) AS hca_gap
FROM fact_team_game ftg
JOIN dim_team   dt ON ftg.team_id  = dt.team_id
JOIN dim_season ds ON ftg.season_id = ds.season_id
GROUP BY ds.season_start_year, ds.season_id, dt.conference
ORDER BY ds.season_start_year, dt.conference
"""

# Q9: Does home court advantage shrink against stronger opponents?
Q9_OPPONENT_STRENGTH = """
WITH season_w_pct AS (
    SELECT
        team_id,
        season_id,
        AVG(CASE WHEN wl = 'W' THEN 1.0 ELSE 0.0 END) AS season_w_pct
    FROM fact_team_game
    GROUP BY team_id, season_id
),
tiered AS (
    SELECT
        team_id,
        season_id,
        season_w_pct,
        NTILE(3) OVER (PARTITION BY season_id ORDER BY season_w_pct DESC) AS strength_tier
    FROM season_w_pct
)
SELECT
    CASE t.strength_tier
        WHEN 1 THEN 'Top Third'
        WHEN 2 THEN 'Middle Third'
        WHEN 3 THEN 'Bottom Third'
    END AS opponent_tier,
    ROUND(AVG(CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END), 4) AS home_w_pct,
    COUNT(*) AS games
FROM fact_team_game ftg
JOIN tiered t ON ftg.opponent_team_id = t.team_id AND ftg.season_id = t.season_id
WHERE ftg.is_home = TRUE
GROUP BY t.strength_tier
ORDER BY t.strength_tier
"""

# Q10: Do teams commit fewer turnovers at home, and does that correlate with higher win%?
Q10_TURNOVER_COMFORT = """
WITH team_tov AS (
    SELECT
        ftg.team_id,
        AVG(CASE WHEN ftg.is_home = TRUE  THEN ftg.tov  END) AS home_tov,
        AVG(CASE WHEN ftg.is_home = FALSE THEN ftg.tov  END) AS road_tov,
        AVG(CASE WHEN ftg.is_home = TRUE  THEN CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END END) AS home_w_pct,
        AVG(CASE WHEN ftg.is_home = FALSE THEN CASE WHEN ftg.wl = 'W' THEN 1.0 ELSE 0.0 END END) AS road_w_pct
    FROM fact_team_game ftg
    GROUP BY ftg.team_id
)
SELECT
    dt.full_name,
    dt.conference,
    ROUND(tt.road_tov  - tt.home_tov,  2) AS tov_differential,
    ROUND(tt.home_w_pct - tt.road_w_pct, 4) AS w_pct_differential,
    RANK() OVER (ORDER BY (tt.road_tov - tt.home_tov)   DESC) AS tov_rank,
    RANK() OVER (ORDER BY (tt.home_w_pct - tt.road_w_pct) DESC) AS w_pct_rank
FROM team_tov tt
JOIN dim_team dt ON tt.team_id = dt.team_id
ORDER BY tov_rank
"""

QUERIES = {
    "q1_baseline_win_pct":     ("baseline",             Q1_BASELINE_WIN_PCT),
    "q2_hca_trend":            ("trend",                Q2_HCA_TREND),
    "q3_covid_bubble":         ("natural_experiments",  Q3_COVID_BUBBLE),
    "q4_back_to_back":         ("scheduling",           Q4_BACK_TO_BACK),
    "q5_shooting_boost":       ("performance",          Q5_SHOOTING_BOOST),
    "q6_rolling_home_win_pct": ("performance",          Q6_ROLLING_HOME_WIN_PCT),
    "q7_referee_bias":         ("scheduling",           Q7_REFEREE_BIAS),
    "q8_conference_hca":       ("context",              Q8_CONFERENCE_HCA),
    "q9_opponent_strength":    ("context",              Q9_OPPONENT_STRENGTH),
    "q10_turnover_comfort":    ("performance",          Q10_TURNOVER_COMFORT),
}