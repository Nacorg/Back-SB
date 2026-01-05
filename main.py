from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import statsbombpy as sb
import pandas as pd
from typing import List, Dict, Any

app = FastAPI(title="StatsBomb Football API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Competition mappings: FD code -> StatsBomb ID
COMPETITION_MAPPINGS = {
    "WC": 43,   # FIFA World Cup
    "CL": 16,   # UEFA Champions League
    "BL1": 9,   # Bundesliga
    "PD": 11,   # Primera Division
    "FL1": 7,   # Ligue 1
    "PL": 2,    # Premier League
    "SA": 12,   # Serie A
    "EC": 68,   # European Championship
    "ELC": 35,  # UEFA Europa League
    # Note: DED (Eredivisie), BSA (Campeonato Brasileiro Série A), PPL (Primeira Liga) not available in StatsBomb open data
}

@app.get("/")
def read_root():
    return {"message": "StatsBomb Football API", "status": "running"}

@app.get("/api/statsbomb/competitions")
def get_competitions():
    """Get available competitions"""
    try:
        competitions = sb.competitions()
        return {"success": True, "data": competitions.to_dict('records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/statsbomb/matches/{competition_code}")
def get_matches(competition_code: str, season_id: int = 2023):
    """Get matches for a competition"""
    try:
        comp_id = COMPETITION_MAPPINGS.get(competition_code.upper())
        if not comp_id:
            return {"success": False, "error": f"Competition {competition_code} not found"}

        matches = sb.matches(competition_id=comp_id, season_id=season_id)
        return {"success": True, "data": matches.to_dict('records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/statsbomb/events/{match_id}")
def get_match_events(match_id: int):
    """Get detailed events for a match"""
    try:
        events = sb.events(match_id=match_id)

        # Process events to include advanced stats
        processed_events = []
        for _, event in events.iterrows():
            processed_event = {
                "id": event.get("id"),
                "type": event.get("type"),
                "team": event.get("team"),
                "player": event.get("player"),
                "minute": event.get("minute"),
                "second": event.get("second"),
                "location": event.get("location"),  # [x, y] coordinates
                "shot": event.get("shot", {}),
                "pass": event.get("pass", {}),
                "carry": event.get("carry", {}),
                "duel": event.get("duel", {}),
                "tactics": event.get("tactics", {}),
                "goalkeeper": event.get("goalkeeper", {}),
                "foul_committed": event.get("foul_committed", {}),
                "foul_won": event.get("foul_won", {}),
                "ball_receipt": event.get("ball_receipt", {}),
                "ball_recovery": event.get("ball_recovery", {}),
                "interception": event.get("interception", {}),
                "clearance": event.get("clearance", {}),
                "dribble": event.get("dribble", {}),
                "block": event.get("block", {}),
                "miscontrol": event.get("miscontrol", {}),
                "dispossessed": event.get("dispossessed", {}),
            }
            processed_events.append(processed_event)

        return {"success": True, "data": processed_events}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/statsbomb/lineups/{match_id}")
def get_match_lineups(match_id: int):
    """Get match lineups"""
    try:
        lineups = sb.lineups(match_id=match_id)
        return {"success": True, "data": lineups}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/statsbomb/player-stats/{match_id}")
def get_player_stats(match_id: int):
    """Get detailed player statistics for a match"""
    try:
        events = sb.events(match_id=match_id)

        # Aggregate player stats
        player_stats = {}

        for _, event in events.iterrows():
            player = event.get("player")
            if not player:
                continue

            if player not in player_stats:
                player_stats[player] = {
                    "name": player,
                    "team": event.get("team"),
                    "minutes_played": 0,
                    "goals": 0,
                    "assists": 0,
                    "shots": 0,
                    "shots_on_target": 0,
                    "passes_completed": 0,
                    "passes_attempted": 0,
                    "tackles": 0,
                    "interceptions": 0,
                    "duels_won": 0,
                    "duels_lost": 0,
                    "yellow_cards": 0,
                    "red_cards": 0,
                    "xg": 0.0,
                    "xa": 0.0,
                }

            # Count different event types
            event_type = event.get("type")

            if event_type == "Shot":
                player_stats[player]["shots"] += 1
                shot = event.get("shot", {})
                if shot.get("outcome", {}).get("name") in ["Goal", "Saved", "Post"]:
                    player_stats[player]["shots_on_target"] += 1
                if "statsbomb_xg" in shot:
                    player_stats[player]["xg"] += shot["statsbomb_xg"]

            elif event_type == "Pass":
                pass_event = event.get("pass", {})
                if pass_event.get("outcome", {}).get("name") != "Incomplete":
                    player_stats[player]["passes_completed"] += 1
                player_stats[player]["passes_attempted"] += 1

                # Check for assists
                if pass_event.get("goal_assist") == True:
                    player_stats[player]["assists"] += 1
                    if "statsbomb_xg" in pass_event:
                        player_stats[player]["xa"] += pass_event["statsbomb_xg"]

            elif event_type == "Goal":
                player_stats[player]["goals"] += 1

            elif event_type == "Tackle":
                player_stats[player]["tackles"] += 1

            elif event_type == "Interception":
                player_stats[player]["interceptions"] += 1

            elif event_type == "Duel":
                duel = event.get("duel", {})
                if duel.get("outcome", {}).get("name") == "Won":
                    player_stats[player]["duels_won"] += 1
                else:
                    player_stats[player]["duels_lost"] += 1

            elif event_type == "Foul Committed":
                foul = event.get("foul_committed", {})
                card = foul.get("card", {})
                if card.get("name") == "Yellow Card":
                    player_stats[player]["yellow_cards"] += 1
                elif card.get("name") == "Red Card":
                    player_stats[player]["red_cards"] += 1

        return {"success": True, "data": list(player_stats.values())}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/statsbomb/team-stats/{match_id}")
def get_team_stats(match_id: int):
    """Get team statistics for a match"""
    try:
        events = sb.events(match_id=match_id)

        team_stats = {}

        for _, event in events.iterrows():
            team = event.get("team")
            if not team:
                continue

            if team not in team_stats:
                team_stats[team] = {
                    "team": team,
                    "goals": 0,
                    "shots": 0,
                    "shots_on_target": 0,
                    "passes_completed": 0,
                    "passes_attempted": 0,
                    "tackles": 0,
                    "interceptions": 0,
                    "duels_won": 0,
                    "duels_lost": 0,
                    "corners": 0,
                    "fouls": 0,
                    "yellow_cards": 0,
                    "red_cards": 0,
                    "xg": 0.0,
                }

            event_type = event.get("type")

            if event_type == "Shot":
                team_stats[team]["shots"] += 1
                shot = event.get("shot", {})
                if shot.get("outcome", {}).get("name") in ["Goal", "Saved", "Post"]:
                    team_stats[team]["shots_on_target"] += 1
                if "statsbomb_xg" in shot:
                    team_stats[team]["xg"] += shot["statsbomb_xg"]

            elif event_type == "Pass":
                pass_event = event.get("pass", {})
                if pass_event.get("outcome", {}).get("name") != "Incomplete":
                    team_stats[team]["passes_completed"] += 1
                team_stats[team]["passes_attempted"] += 1

            elif event_type == "Goal":
                team_stats[team]["goals"] += 1

            elif event_type == "Tackle":
                team_stats[team]["tackles"] += 1

            elif event_type == "Interception":
                team_stats[team]["interceptions"] += 1

            elif event_type == "Duel":
                duel = event.get("duel", {})
                if duel.get("outcome", {}).get("name") == "Won":
                    team_stats[team]["duels_won"] += 1
                else:
                    team_stats[team]["duels_lost"] += 1

            elif event_type == "Corner":
                team_stats[team]["corners"] += 1

            elif event_type == "Foul Committed":
                team_stats[team]["fouls"] += 1
                foul = event.get("foul_committed", {})
                card = foul.get("card", {})
                if card.get("name") == "Yellow Card":
                    team_stats[team]["yellow_cards"] += 1
                elif card.get("name") == "Red Card":
                    team_stats[team]["red_cards"] += 1

        return {"success": True, "data": list(team_stats.values())}
    except Exception as e:
        return {"success": False, "error": str(e)}
