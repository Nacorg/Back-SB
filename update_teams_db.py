import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import statsbombpy as sb
from supabase import create_client, Client
from dotenv import load_dotenv
import ast

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_teams_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StatsBombUpdater:
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.comp_ids = ast.literal_eval(os.getenv('COMP_IDS', '[11]'))

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env file")

        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

        # Competitions to process (StatsBomb IDs)
        self.comp_ids = [43, 16, 9, 11, 7, 2, 12, 68, 35]  # WC, CL, BL1, PD, FL1, PL, SA, EC, ELC

    def get_last_update_date(self) -> datetime:
        """Get the date of the last match in the database"""
        try:
            result = self.supabase.table('matches').select('date').order('date', desc=True).limit(1).execute()
            if result.data:
                return datetime.fromisoformat(result.data[0]['date'])
            return datetime.now() - timedelta(days=365*2)  # Default to 2 years ago if no data
        except Exception as e:
            logger.warning(f"Could not get last update date: {e}")
            return datetime.now() - timedelta(days=365*2)

    def get_competitions(self) -> pd.DataFrame:
        """Get available competitions from StatsBomb"""
        try:
            competitions = sb.competitions()
            logger.info(f"Retrieved {len(competitions)} competitions")
            return competitions
        except Exception as e:
            logger.error(f"Error getting competitions: {e}")
            return pd.DataFrame()

    def get_recent_seasons(self, comp_id: int, num_seasons: int = 2) -> List[int]:
        """Get the most recent season IDs for a competition"""
        try:
            matches = sb.matches(competition_id=comp_id)
            seasons = matches['season_id'].unique()
            recent_seasons = sorted(seasons, reverse=True)[:num_seasons]
            logger.info(f"Recent seasons for comp {comp_id}: {recent_seasons}")
            return recent_seasons
        except Exception as e:
            logger.error(f"Error getting seasons for comp {comp_id}: {e}")
            return []

    def get_all_matches(self, comp_id: int, season_id: int) -> pd.DataFrame:
        """Get all matches in a season"""
        try:
            matches = sb.matches(competition_id=comp_id, season_id=season_id)
            logger.info(f"Found {len(matches)} matches in season {season_id} for competition {comp_id}")
            return matches
        except Exception as e:
            logger.error(f"Error getting matches for competition {comp_id}: {e}")
            return pd.DataFrame()

    def get_match_events(self, match_id: int) -> pd.DataFrame:
        """Get events for a specific match"""
        try:
            events = sb.events(match_id=match_id)
            logger.info(f"Retrieved {len(events)} events for match {match_id}")
            return events
        except Exception as e:
            logger.error(f"Error getting events for match {match_id}: {e}")
            return pd.DataFrame()

    def get_match_lineups(self, match_id: int) -> Dict[str, pd.DataFrame]:
        """Get lineups for a specific match"""
        try:
            lineups = sb.lineups(match_id=match_id)
            logger.info(f"Retrieved lineups for match {match_id}")
            return lineups
        except Exception as e:
            logger.error(f"Error getting lineups for match {match_id}: {e}")
            return {}

    def upsert_teams(self, teams_data: List[Dict[str, Any]]) -> None:
        """Upsert teams into Supabase"""
        try:
            self.supabase.table('teams').upsert(teams_data, on_conflict='id').execute()
            logger.info(f"Upserted {len(teams_data)} teams")
        except Exception as e:
            logger.error(f"Error upserting teams: {e}")

    def upsert_matches(self, matches_data: List[Dict[str, Any]]) -> None:
        """Upsert matches into Supabase"""
        try:
            self.supabase.table('matches').upsert(matches_data, on_conflict='id').execute()
            logger.info(f"Upserted {len(matches_data)} matches")
        except Exception as e:
            logger.error(f"Error upserting matches: {e}")

    def upsert_competition_standings(self, standings_data: List[Dict[str, Any]]) -> None:
        """Upsert competition standings into Supabase"""
        try:
            self.supabase.table('competition_standings').upsert(standings_data, on_conflict='id').execute()
            logger.info(f"Upserted {len(standings_data)} competition standings")
        except Exception as e:
            logger.error(f"Error upserting competition standings: {e}")

    def upsert_competitions(self, competitions_data: List[Dict[str, Any]]) -> None:
        """Upsert competitions into Supabase"""
        try:
            self.supabase.table('competitions').upsert(competitions_data, on_conflict='id').execute()
            logger.info(f"Upserted {len(competitions_data)} competitions")
        except Exception as e:
            logger.error(f"Error upserting competitions: {e}")

    def upsert_match_player_stats(self, stats_data: List[Dict[str, Any]]) -> None:
        """Upsert match player stats into Supabase"""
        try:
            self.supabase.table('match_player_stats').upsert(stats_data, on_conflict='id').execute()
            logger.info(f"Upserted {len(stats_data)} match player stats")
        except Exception as e:
            logger.error(f"Error upserting match player stats: {e}")

    def upsert_players(self, players_data: List[Dict[str, Any]]) -> None:
        """Upsert players into Supabase"""
        try:
            self.supabase.table('players').upsert(players_data, on_conflict='id').execute()
            logger.info(f"Upserted {len(players_data)} players")
        except Exception as e:
            logger.error(f"Error upserting players: {e}")

    def process_match_data(self, match: pd.Series) -> None:
        """Process all data for a single match"""
        match_id = match['match_id']

        # Get events and lineups
        events_df = self.get_match_events(match_id)
        lineups = self.get_match_lineups(match_id)

        if events_df.empty:
            logger.warning(f"No events found for match {match_id}")
            return

        # Extract teams from match
        teams = []
        # Map competition IDs to countries
        comp_country_map = {
            43: 'International',  # WC
            16: 'Europe',  # CL
            9: 'Germany',  # BL1
            11: 'Spain',  # PD
            7: 'France',  # FL1
            2: 'England',  # PL
            12: 'Italy',  # SA
            68: 'Europe',  # EC
            35: 'Europe'  # ELC
        }
        country = comp_country_map.get(match.get('competition_id'), 'Unknown')

        home_team = {
            'id': match['home_team_id'],
            'name': match['home_team'],
            'country': country
        }
        away_team = {
            'id': match['away_team_id'],
            'name': match['away_team'],
            'country': country
        }
        teams.extend([home_team, away_team])

        # Extract players from lineups
        players = []
        for team_name, lineup_df in lineups.items():
            for _, player in lineup_df.iterrows():
                player_data = {
                    'id': player['player_id'],
                    'name': player['player_name'],
                    'team_id': match['home_team_id'] if team_name == match['home_team'] else match['away_team_id']
                }
                players.append(player_data)

        # Prepare match data
        match_data = {
            'id': match_id,
            'competition_id': match.get('competition_id'),
            'matchday': match.get('match_week'),
            'date': match['match_date'].isoformat() if hasattr(match['match_date'], 'isoformat') else str(match['match_date']),
            'home_team_id': match['home_team_id'],
            'away_team_id': match['away_team_id'],
            'home_score': match.get('home_score'),
            'away_score': match.get('away_score'),
            'status': match.get('match_status', 'completed')
        }

        # Prepare match player stats data
        match_player_stats = []
        for _, event in events_df.iterrows():
            if event.get('player_id') and event.get('type') in ['Pass', 'Shot', 'Dribble', 'Carry']:
                stat_data = {
                    'match_id': match_id,
                    'player_id': event['player_id'],
                    'minutes': event.get('minute', 0),
                    'goals': 1 if event.get('shot_outcome') == 'Goal' else 0,
                    'assists': 1 if event.get('pass_goal_assist') == True else 0,
                    'rating': event.get('player_rating', 0)
                }
                match_player_stats.append(stat_data)

        # Upsert data
        self.upsert_teams(teams)
        self.upsert_matches([match_data])
        self.upsert_players(players)
        self.upsert_match_player_stats(match_player_stats)

    def update_database(self) -> None:
        """Main method to update the database with new StatsBomb data"""
        logger.info("Starting database update process")

        last_update = self.get_last_update_date()
        logger.info(f"Last update date: {last_update}")

        competitions = self.get_competitions()
        if competitions.empty:
            logger.error("No competitions retrieved")
            return

        for comp_id in self.comp_ids:
            if comp_id not in competitions['competition_id'].values:
                logger.warning(f"Competition {comp_id} not found in available competitions")
                continue

            recent_seasons = self.get_recent_seasons(comp_id)
            for season_id in recent_seasons:
                matches = self.get_matches_for_teams(comp_id, season_id, self.key_teams)
                for _, match in matches.iterrows():
                    match_date = pd.to_datetime(match['match_date'])
                    if match_date > last_update:
                        logger.info(f"Processing match {match['match_id']} from {match_date}")
                        self.process_match_data(match)
                    else:
                        logger.info(f"Skipping match {match['match_id']} (already up to date)")

        logger.info("Database update process completed")

def main():
    """Main entry point"""
    try:
        updater = StatsBombUpdater()
        updater.update_database()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main()
