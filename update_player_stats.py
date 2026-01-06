import os
import pandas as pd
import statsbombpy as sb
from supabase import create_client, Client
from typing import Dict, List
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_all_players() -> List[Dict]:
    """Get all players from the database"""
    response = supabase.table('players').select('*').execute()
    return response.data

def get_player_matches(player_id: int) -> List[Dict]:
    """Get all matches where a player participated"""
    # Get match_player_stats for this player
    response = supabase.table('match_player_stats').select('match_id, rating').eq('player_id', player_id).execute()
    return response.data

def calculate_player_stats(player_id: int) -> Dict:
    """Calculate best rating, worst rating, yellow cards, red cards for a player"""
    matches = get_player_matches(player_id)

    if not matches:
        return {
            'best_rating': 0.0,
            'worst_rating': 0.0,
            'yellow_cards': 0,
            'red_cards': 0
        }

    ratings = [match['rating'] for match in matches if match['rating'] is not None and match['rating'] > 0]

    # Calculate best and worst ratings
    best_rating = max(ratings) if ratings else 0.0
    worst_rating = min(ratings) if ratings else 0.0

    # For cards, we need to aggregate from match events
    # This is a simplified version - in a real implementation,
    # you'd want to store card events in the database
    yellow_cards = 0
    red_cards = 0

    # For now, we'll set cards to 0 and note that this needs to be implemented
    # based on actual match events data
    logger.warning(f"Card counting not implemented yet for player {player_id}")

    return {
        'best_rating': best_rating,
        'worst_rating': worst_rating,
        'yellow_cards': yellow_cards,
        'red_cards': red_cards
    }

def update_player_stats(player_id: int, stats: Dict):
    """Update player stats in the database"""
    try:
        supabase.table('players').update({
            'best_rating': stats['best_rating'],
            'worst_rating': stats['worst_rating'],
            'yellow_cards': stats['yellow_cards'],
            'red_cards': stats['red_cards']
        }).eq('id', player_id).execute()

        logger.info(f"Updated stats for player {player_id}: {stats}")
    except Exception as e:
        logger.error(f"Failed to update player {player_id}: {e}")

def main():
    """Main function to update all players' stats"""
    logger.info("Starting player stats update...")

    players = get_all_players()
    logger.info(f"Found {len(players)} players to update")

    for player in players:
        player_id = player['id']
        logger.info(f"Processing player {player_id} ({player.get('name', 'Unknown')})")

        stats = calculate_player_stats(player_id)
        update_player_stats(player_id, stats)

    logger.info("Player stats update completed!")

if __name__ == "__main__":
    main()
