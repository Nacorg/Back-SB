# StatsBomb Backend API

Backend service using FastAPI and StatsBomb open data for detailed football statistics.

## 🚀 Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python main.py
```

The API will be available at `http://localhost:8000`

### Endpoints

- `GET /` - Health check
- `GET /api/statsbomb/matches/{competition_code}` - Get matches for a competition
- `GET /api/statsbomb/events/{match_id}` - Get detailed events for a match
- `GET /api/statsbomb/lineups/{match_id}` - Get match lineups
- `GET /api/statsbomb/player-stats/{match_id}` - Get player statistics
- `GET /api/statsbomb/competitions` - Get available competitions

### Competition Codes
- `WC` - FIFA World Cup
- `CL` - UEFA Champions League
- `BL1` - Bundesliga
- `PD` - Primera Division
- `FL1` - Ligue 1
- `PL` - Premier League
- `SA` - Serie A
- `EC` - European Championship
- `ELC` - UEFA Europa League

*Note: DED (Eredivisie), BSA (Campeonato Brasileiro Série A), PPL (Primeira Liga) are not available in StatsBomb open data.*

## 📊 Data Available

### Match Events
- Goals, assists, shots
- xG (expected goals)
- Pass completion rates
- Defensive actions (tackles, interceptions)
- Duels won/lost
- Cards (yellow/red)

### Player Stats
- Minutes played
- Goals and assists
- Shot accuracy
- Pass completion
- Defensive stats
- Duel success rate

## 🚀 Deployment (Free)

### Railway (Recommended)
1. Create account at [railway.app](https://railway.app)
2. Connect your GitHub repository
3. Railway auto-detects Python and deploys
4. Get free URL: `https://your-project.up.railway.app`

### Alternative Free Options
- **Render**: 750 hours/month free
- **Fly.io**: Generous free tier

## 🔗 Integration with Flutter

Update your `football_data_service.dart`:

```dart
static const String _statsBombUrl = "https://your-backend.up.railway.app";

// In getMatchHybrid method, for post-match:
if (status == "FINISHED") {
  final sbResponse = await http.get(
    Uri.parse("$_statsBombUrl/api/statsbomb/events/$matchId")
  );
  final sbData = jsonDecode(sbResponse.body);
  if (sbData["success"]) {
    return {
      "phase": "post",
      "events": sbData["data"],
      "api": "statsbomb"
    };
  }
}
```

## 📋 Requirements

- Python 3.8+
- StatsBomb open data (free)
- FastAPI for the web framework
