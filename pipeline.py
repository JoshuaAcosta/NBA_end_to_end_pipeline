import glob
import json
import logging
import os
import pandas as pd
from pathlib import Path
import time
from dotenv import load_dotenv
load_dotenv()
import duckdb
from nba_api.stats.endpoints import LeagueDashTeamStats
from nba_api.stats.endpoints import LeagueGameFinder
from nba_api.stats.static import teams

# Logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class NBADataPipeline():

    '''
    Extracting NBA data, loading it to DuckDB, and transforming it into 
    a star schema for home court advantage analysis
    '''

    def __init__(self, data_dir: str, start_season: int, end_season: int):
        self.data_dir = data_dir
        self.start_season = start_season
        self.end_season = end_season
        self.logger = logger

        self.raw_dir = Path(self.data_dir)
        self.team_static_path = Path(self.data_dir + "/team_static")
        self.game_logs_path = Path(self.data_dir + "/game_logs")
        self.team_stats_path = Path(self.data_dir + "/team_stats")
        self.db_path = Path(self.data_dir + "/db")
        self.db_file = Path(self.db_path, "nba_analytics.duckdb")

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.team_static_path.mkdir(parents=True, exist_ok=True)
        self.game_logs_path.mkdir(parents=True, exist_ok=True)
        self.team_stats_path.mkdir(parents=True, exist_ok=True)
        self.db_path.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(self.db_file)

        self.logger.info("NBADataPipeline initialized")

    def _season_formatter(self, year):
        return f"{year}-{str(year+1)[2:]}"

    def extract_team_data(self):
        
        try:
            self.logger.info("Extracting static team data")
            team_data = teams.get_teams()
            self.logger.info(f"Extracted {len(team_data)} rows")

            file_path = os.path.join(self.team_static_path, 'teams_static.json')
            self.logger.info(f"Writing data to JSON file at {file_path} ")
            with open(file_path, 'w') as f:
                json.dump(team_data, f, indent=4)
            self.logger.info(f"Successfully wrote data to JSON file at {file_path}")
        
        except Exception as e:
            self.logger.error(f"Failed to extract static team data: {e}")
            raise
    
    def extract_game_logs(self):
        try:
            for season in range(self.start_season, self.end_season+1):
                try:
                    full_season = self._season_formatter(season)

                    self.logger.info(f"Extracting game log data for {full_season} season")
                    game_log = LeagueGameFinder(season_nullable=full_season,
                                                season_type_nullable='Regular Season').get_normalized_dict()["LeagueGameFinderResults"]
                    self.logger.info(f"Extracted {len(game_log)} rows")

                    file_path = os.path.join(self.game_logs_path, f'{full_season}_game_logs.json')
                    self.logger.info(f"Writing data to JSON file at {file_path} ")

                    with open(file_path, 'w') as f:
                        json.dump(game_log, f, indent=4)
                    
                    self.logger.info(f"Successfully wrote data to JSON file at {file_path}")
                    
                    time.sleep(1)
                
                except Exception as e:
                    self.logger.error(f"Failed to extract game log data: {e}")

        except Exception as e:
            self.logger.error(f"Failed to extract game log data: {e}")
            raise

    def extract_team_season_stats(self, game_location, metrics_level):
        try:
            for season in range(self.start_season, self.end_season+1):
                try:
                    full_season = self._season_formatter(season)

                    self.logger.info(f"Extracting team season stats for {full_season} season")
                    headers = {
                        "Host": "stats.nba.com",
                        "Connection": "keep-alive",
                        "Accept": "application/json, text/plain, /",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                        "Referer": "https://www.nba.com/",
                        "Origin": "https://www.nba.com",
                        "x-nba-stats-origin": "stats",
                        "x-nba-stats-token": "true",
                        "Accept-Language": "en-US,en;q=0.9"
                        }
                    team_stats = LeagueDashTeamStats(season=full_season,
                                                     location_nullable=game_location,
                                                     measure_type_detailed_defense=metrics_level,
                                                     timeout=100).get_normalized_dict()["LeagueDashTeamStats"]
                    self.logger.info(f"Extracted {len(team_stats)} rows")

                    file_path = os.path.join(self.team_stats_path, f'{full_season}_{game_location}_{metrics_level}_team_stats.json')
                    self.logger.info(f"Writing data to JSON file at {file_path} ")

                    with open(file_path, 'w') as f:
                        json.dump(team_stats, f, indent=4)
                    
                    self.logger.info(f"Successfully wrote data to JSON file at {file_path}")
                    
                    time.sleep(3)
                
                except Exception as e:
                    self.logger.error(f"Failed to extract team season stats data: {e}")

        except Exception as e:
            self.logger.error(f"Failed to extract team season stats data: {e}")
            raise

    def load_team_data(self):
        try:
            team_data_path = f'{self.team_static_path}/*.json'
            self.logger.info(f"Loading JSON files in duckdb staging table from {team_data_path}")
            self.conn.execute(f'CREATE TABLE staging_teams_data AS SELECT * FROM read_json_auto(\'{team_data_path}\')')

            rows_loaded = self.conn.execute("SELECT COUNT(*) FROM staging_teams_data").fetchone()[0]
            self.logger.info(f"Successfully loaded {rows_loaded} into game log staging table from {team_data_path}")

        except Exception as e:
            self.logger.error(f"Failed to load team season stats data: {e}")
            raise

    def load_game_logs_data(self):
        try:
            game_data_path = f'{self.game_logs_path}/*.json'
            self.logger.info(f"Loading JSON files in duckdb staging table from {game_data_path}")
            self.conn.execute(f'CREATE TABLE staging_game_log_data AS SELECT * FROM read_json_auto(\'{game_data_path}\')')

            rows_loaded = self.conn.execute("SELECT COUNT(*) FROM staging_game_log_data").fetchone()[0]
            self.logger.info(f"Successfully loaded {rows_loaded} into game log staging table from {game_data_path}")
        except Exception as e:
            self.logger.error(f"Failed to load game logs data: {e}")
            raise

    def load_team_season_data(self):
        try: 
            base_dfs = []
            advanced_dfs = []
            team_stats_path = glob.glob(f'{self.team_stats_path}/*.json')

            for file_path in team_stats_path:
                df = pd.read_json(file_path)
                file_base_name = os.path.basename(file_path)
                df["season"] = os.path.basename(file_base_name)[:7]

                if "Home" in file_base_name:
                    df["location"] = "Home"
                elif "Road" in file_base_name:
                    df["location"] = "Road"

                if "Base" in file_base_name:
                    base_dfs.append(df)
                elif "Advanced" in file_base_name:
                    advanced_dfs.append(df)

            combined_base = pd.concat(base_dfs)
            combined_advanced = pd.concat(advanced_dfs)

            self.conn.execute('CREATE TABLE staging_team_stats_base AS SELECT * FROM combined_base')
            base_rows_loaded = self.conn.execute("SELECT COUNT(*) FROM staging_team_stats_base").fetchone()[0]
            self.logger.info(f"Successfully loaded {base_rows_loaded} into team season staging table from {file_base_name}")
    
            self.conn.execute('CREATE TABLE staging_team_stats_advanced AS SELECT * FROM combined_advanced')
            adv_rows_loaded = self.conn.execute("SELECT COUNT(*) FROM staging_team_stats_advanced").fetchone()[0]
            self.logger.info(f"Successfully loaded {adv_rows_loaded} into team season staging table from {file_base_name}")

        except Exception as e:
            self.logger.error(f"Failed to load team season data: {e}")
            raise

    def transform_team_data(self):
        try:
            self.logger.info(f"Transforming and loading team data to dim_team table")
            self.conn.execute("""
                            CREATE TABLE dim_team AS 
                            SELECT 
                                id, 
                                full_name, 
                                abbreviation, 
                                nickname, 
                                city, 
                                state, 
                                year_founded,
                                CASE
                                    WHEN abbreviation in ('BOS','BKN','NYK','PHI','TOR','CHI','CLE','DET','IND',
                                    'MIL','ATL','CHA','MIA','ORL','WAS') THEN 'East' ELSE 'West' END AS conference 
                            FROM staging_teams_data
                            """)
            dim_team_row_count = self.conn.execute("SELECT COUNT(*) FROM dim_team").fetchone()[0]
            self.logger.info(f"Loaded {dim_team_row_count} rows of team data to dim_team table")
        except Exception as e:
            self.logger.error(f"Failed to load dim_team data: {e}")
            raise

    def transform_dim_season(self):
        try:
            self.logger.info("Creating dim_season table in db")
            self.conn.execute(
            """CREATE TABLE dim_season (
                season_id VARCHAR PRIMARY KEY, 
                season_start_year INTEGER NOT NULL,
                era VARCHAR NOT NULL,
                games_in_season INTEGER NOT NULL,
                is_lockout BOOLEAN NOT NULL,
                is_COVID BOOLEAN NOT NULL
                )
            """
            )
            self.logger.info("Inserting data into dim_season table in db")
            self.conn.execute(""" INSERT INTO dim_season
                            SELECT 
                                DISTINCT CONCAT(SEASON_ID[2:], '-', LPAD(CAST((CAST(SEASON_ID[2:] AS INTEGER) + 1) % 100 AS VARCHAR), 2, '0')) as season_id,
                                CAST(SEASON_ID[2:] AS INTEGER) as season_start_year,
                                CASE
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) BETWEEN 1996 and 2016 THEN 'Pre-Back-to-Back Reform'
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) BETWEEN 2017 and 2018 THEN 'Schedule Reform'
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) = 2019 THEN 'COVID Bubble'
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) = 2020 THEN 'COVID Compressed'
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) BETWEEN 2021 and 2024 THEN 'Post-COVID'
                                    ELSE Null END AS era,
                                CASE
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) = 1998 THEN 50
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) = 2011 THEN 66
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) = 2019 THEN 75
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) = 2020 THEN 72
                                    ELSE 82 END AS games_in_season,
                                CASE
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) in (1998, 2011) THEN TRUE
                                    ELSE FALSE END AS is_lockout,
                                CASE
                                    WHEN CAST(SEASON_ID[2:] AS INTEGER) in (2019,2020) THEN TRUE
                                    ELSE FALSE END AS is_covid
                            
                                FROM staging_game_log_data
                            """)
            
            dim_season_row_count = self.conn.execute("SELECT COUNT(*) FROM dim_season").fetchone()[0]
            self.logger.info(f"Loaded {dim_season_row_count} rows of team data to dim_team table")

        except Exception as e:
            self.logger.error(f"Failed to transform dim_season data: {e}")
            raise

    def transform_dim_game(self):
        try:
            self.logger.info("Creating dim_game table in db")
            self.conn.execute(
            """CREATE TABLE dim_game (
                game_id VARCHAR PRIMARY KEY,
                game_date DATE NOT NULL,
                month INTEGER, 
                day_of_week VARCHAR, 
                season_id VARCHAR NOT NULL,
                is_bubble BOOLEAN
                )
            """
            )
            self.logger.info("Inserting data into dim_game table in db")
            self.conn.execute(""" INSERT INTO dim_game
                            SELECT 
                                DISTINCT game_id,
                                CAST(game_date AS DATE) as game_date,
                                EXTRACT(MONTH from game_date) as month,
                                DAYNAME(game_date) as day_of_week,
                                CONCAT(SEASON_ID[2:], '-', LPAD(CAST((CAST(SEASON_ID[2:] AS INTEGER) + 1) % 100 AS VARCHAR), 2, '0')) as season_id,
                                CASE 
                                    WHEN game_date BETWEEN '2020-07-30' AND '2020-10-11' THEN TRUE ELSE FALSE END AS is_bubble
                            FROM staging_game_log_data 
                            """)
            
            dim_game_row_count = self.conn.execute("SELECT COUNT(*) FROM dim_game").fetchone()[0]
            self.logger.info(f"Loaded {dim_game_row_count} rows of team data to dim_game table")
            
        except Exception as e:
            self.logger.error(f"Failed to transform dim_game data: {e}")
            raise

def main():
    logger.info("Starting data pipeline script")
    DATA_DIR = os.getenv('DATA_DIR')
    pl = NBADataPipeline(DATA_DIR, 1996, 2024)
    #pl.extract_team_data()
    #pl.extract_game_logs()
    #pl.extract_team_season_stats('Home', 'Base')
    #pl.extract_team_season_stats('Home', 'Advanced')
    #pl.extract_team_season_stats('Road', 'Base')
    #pl.extract_team_season_stats("Road", "Advanced")
    #pl.load_team_data()
    #pl.load_game_logs_data()
    #pl.load_team_season_data()
    #pl.transform_team_data()
    #pl.transform_dim_season()
    #pl.transform_dim_game()
    
    #pl.conn.execute("DROP TABLE dim_season")
    #print(pl.conn.execute("SELECT * FROM dim_game LIMIT 5").fetchdf())


if __name__ == "__main__":
    main()
