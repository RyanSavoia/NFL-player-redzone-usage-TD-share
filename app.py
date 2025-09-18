from flask import Flask, jsonify, request
import nfl_data_py as nfl
import pandas as pd
import json
import os
import schedule
import time
import threading
from datetime import datetime

app = Flask(__name__)

class PlayerUsageService:
    def __init__(self):
        """Service to calculate player red zone usage and TD shares"""
        self.pbp_data = None
        self.data_loaded = False
        
    def load_nfl_data(self):
        """Load current NFL play-by-play data"""
        if not self.data_loaded:
            print("Loading 2025 NFL play-by-play data...")
            try:
                self.pbp_data = nfl.import_pbp_data([2025])
                self.data_loaded = True
                print(f"Loaded {len(self.pbp_data)} total plays")
            except Exception as e:
                print(f"Error loading NFL data: {str(e)}")
                # Create empty DataFrame to prevent crashes
                self.pbp_data = pd.DataFrame()
                self.data_loaded = True
                # Don't re-raise the exception, just continue with empty data
                return
    
    def get_player_rz_usage_share(self, team):
        """
        Get player red zone usage shares with 2+ plays filter and no 2-pt conversions
        Following GPT's exact specifications
        """
        self.load_nfl_data()
        
        if self.pbp_data.empty:
            return {}
        
        # Filter for red zone plays, excluding 2-point conversions
        rz_data = self.pbp_data[
            (self.pbp_data['posteam'] == team) & 
            (self.pbp_data['yardline_100'] <= 20) & 
            ((self.pbp_data['rush'] == 1) | (self.pbp_data['pass'] == 1)) &
            (self.pbp_data['two_point_attempt'] != 1)  # Exclude 2-pt attempts
        ].copy()
        
        if rz_data.empty:
            return {}
        
        # Apply 2+ plays filter per drive (GPT's requirement)
        filtered_plays = []
        for game_id in rz_data['game_id'].unique():
            if pd.isna(game_id):
                continue
            game_data = rz_data[rz_data['game_id'] == game_id]
            for drive_id in game_data['fixed_drive'].unique():
                if pd.isna(drive_id):
                    continue
                drive_plays = game_data[game_data['fixed_drive'] == drive_id]
                if len(drive_plays) >= 2:  # 2+ plays filter
                    filtered_plays.append(drive_plays)
        
        if not filtered_plays:
            return {}
        
        rz_filtered = pd.concat(filtered_plays, ignore_index=True)
        total_rz_plays = len(rz_filtered)
        usage_shares = {}
        
        # Accumulate rushing usage by player_id then add names (GPT's approach)
        rush_data = rz_filtered[rz_filtered['rush'] == 1]
        if not rush_data.empty:
            rush_counts = rush_data['rusher_player_id'].value_counts()
            for player_id, count in rush_counts.items():
                if pd.notna(player_id):
                    share = count / total_rz_plays
                    # Get player name
                    player_name_series = rush_data[rush_data['rusher_player_id'] == player_id]['rusher_player_name']
                    if not player_name_series.empty:
                        player_name = player_name_series.iloc[0]
                        if pd.notna(player_name):
                            if player_name in usage_shares:
                                usage_shares[player_name] += share
                            else:
                                usage_shares[player_name] = share
        
        # Accumulate receiving usage by player_id then add names
        pass_data = rz_filtered[rz_filtered['pass'] == 1]
        if not pass_data.empty:
            target_counts = pass_data['receiver_player_id'].value_counts()
            for player_id, count in target_counts.items():
                if pd.notna(player_id):
                    share = count / total_rz_plays
                    # Get player name
                    player_name_series = pass_data[pass_data['receiver_player_id'] == player_id]['receiver_player_name']
                    if not player_name_series.empty:
                        player_name = player_name_series.iloc[0]
                        if pd.notna(player_name):
                            if player_name in usage_shares:
                                usage_shares[player_name] += share
                            else:
                                usage_shares[player_name] = share
        
        return usage_shares
    
    def get_player_td_share(self, team):
        """Get player TD shares (accumulating rush + receiving TDs)"""
        self.load_nfl_data()
        
        if self.pbp_data.empty:
            return {}
        
        td_data = self.pbp_data[
            (self.pbp_data['posteam'] == team) & 
            (self.pbp_data['touchdown'] == 1)
        ].copy()
        
        if td_data.empty:
            return {}
        
        total_tds = len(td_data)
        td_shares = {}
        
        # Accumulate rushing TDs by player_id then add names (GPT's approach)
        rush_tds = td_data[td_data['rush'] == 1]
        if not rush_tds.empty:
            rush_td_counts = rush_tds['rusher_player_id'].value_counts()
            for player_id, count in rush_td_counts.items():
                if pd.notna(player_id):
                    share = count / total_tds
                    # Get player name
                    player_name_series = rush_tds[rush_tds['rusher_player_id'] == player_id]['rusher_player_name']
                    if not player_name_series.empty:
                        player_name = player_name_series.iloc[0]
                        if pd.notna(player_name):
                            if player_name in td_shares:
                                td_shares[player_name] += share
                            else:
                                td_shares[player_name] = share
        
        # Accumulate receiving TDs by player_id then add names
        pass_tds = td_data[td_data['pass'] == 1]
        if not pass_tds.empty:
            rec_td_counts = pass_tds['receiver_player_id'].value_counts()
            for player_id, count in rec_td_counts.items():
                if pd.notna(player_id):
                    share = count / total_tds
                    # Get player name
                    player_name_series = pass_tds[pass_tds['receiver_player_id'] == player_id]['receiver_player_name']
                    if not player_name_series.empty:
                        player_name = player_name_series.iloc[0]
                        if pd.notna(player_name):
                            if player_name in td_shares:
                                td_shares[player_name] += share
                            else:
                                td_shares[player_name] = share
        
        return td_shares
    
    def get_team_player_usage(self, team):
        """Get combined player usage data for a team"""
        try:
            rz_usage = self.get_player_rz_usage_share(team)
            td_shares = self.get_player_td_share(team)
            
            # Combine all players mentioned in either metric
            all_players = set(rz_usage.keys()) | set(td_shares.keys())
            
            team_data = {
                'team': team,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'players': {}
            }
            
            for player in all_players:
                rz_share = rz_usage.get(player, 0.0)
                td_share = td_shares.get(player, 0.0)
                
                team_data['players'][player] = {
                    'rz_usage_share': round(rz_share, 4),
                    'td_share': round(td_share, 4)
                }
            
            # Sort players by combined usage (RZ usage + TD share)
            team_data['players'] = dict(sorted(
                team_data['players'].items(),
                key=lambda x: x[1]['rz_usage_share'] + x[1]['td_share'],
                reverse=True
            ))
            
            return team_data
            
        except Exception as e:
            print(f"Error getting team player usage for {team}: {str(e)}")
            return {
                'team': team,
                'error': str(e),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'players': {}
            }
    
    def get_all_teams_usage(self):
        """Get player usage data for all 32 NFL teams"""
        try:
            self.load_nfl_data()
            
            if self.pbp_data is None or self.pbp_data.empty:
                return {
                    'error': 'No NFL data available',
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'teams': {}
                }
            
            # Get all unique teams from current season
            try:
                teams = sorted([team for team in self.pbp_data['posteam'].dropna().unique() if pd.notna(team)])
            except Exception as e:
                return {
                    'error': f'Could not extract teams: {str(e)}',
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'teams': {}
                }
            
            all_teams_data = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'methodology': {
                    'rz_usage_share': 'Player share of team red zone opportunities (rush attempts + targets) with 2+ plays filter, excluding 2-pt conversions',
                    'td_share': 'Player share of team touchdowns (rushing + receiving)',
                    'calculation': 'Uses player IDs to avoid name collisions, accumulates across rush and receiving'
                },
                'teams': {},
                'total_teams_processed': 0
            }
            
            for team in teams:
                try:
                    print(f"Processing {team}...")
                    team_data = self.get_team_player_usage(team)
                    all_teams_data['teams'][team] = team_data
                    all_teams_data['total_teams_processed'] += 1
                except Exception as e:
                    print(f"Error processing team {team}: {str(e)}")
                    all_teams_data['teams'][team] = {
                        'team': team,
                        'error': str(e),
                        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'players': {}
                    }
                    continue
            
            print(f"Completed analysis for {len(teams)} teams")
            return all_teams_data
            
        except Exception as e:
            print(f"Error in get_all_teams_usage: {str(e)}")
            return {
                'error': str(e),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'teams': {}
            }

# Initialize service
player_service = PlayerUsageService()

def refresh_data_job():
    """Scheduled job to refresh data daily"""
    try:
        print("Starting scheduled data refresh...")
        global player_service
        player_service = PlayerUsageService()  # Reset service to reload data
        print("Scheduled data refresh completed successfully")
    except Exception as e:
        print(f"Scheduled data refresh failed: {str(e)}")

def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            print(f"Scheduler error: {str(e)}")
            time.sleep(60)

# Schedule daily refresh at 6 AM UTC
schedule.every().day.at("06:00").do(refresh_data_job)

# Start scheduler in background thread
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()
print("Background scheduler started - daily refresh at 6:00 AM UTC")

@app.route('/player-usage', methods=['GET'])
def get_player_usage():
    """Get player usage data for all teams or specific team"""
    try:
        team = request.args.get('team')  # Optional team parameter
        
        if team:
            # Get data for specific team
            team_data = player_service.get_team_player_usage(team.upper())
            return jsonify(team_data)
        else:
            # Get data for all teams
            all_data = player_service.get_all_teams_usage()
            return jsonify(all_data)
            
    except Exception as e:
        print(f"Error in player usage endpoint: {str(e)}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/player-usage/<team>', methods=['GET'])
def get_team_player_usage_route(team):
    """Get player usage data for a specific team"""
    try:
        team_data = player_service.get_team_player_usage(team.upper())
        return jsonify(team_data)
    except Exception as e:
        print(f"Error in team player usage endpoint for {team}: {str(e)}")
        return jsonify({
            "error": str(e),
            "team": team,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/refresh', methods=['POST'])
def refresh_data():
    """Manual data refresh endpoint"""
    try:
        global player_service
        player_service = PlayerUsageService()  # Reset service to reload data
        return jsonify({
            "status": "success",
            "message": "Data refreshed successfully",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Player Usage Service",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "data_loaded": player_service.data_loaded if player_service else False,
        "scheduled_refresh": "Daily at 6:00 AM UTC",
        "next_refresh": schedule.next_run().strftime('%Y-%m-%d %H:%M:%S UTC') if schedule.jobs else None
    })

@app.route('/', methods=['GET'])
def root():
    """API documentation"""
    return jsonify({
        "service": "Player Usage Service",
        "description": "Calculates player red zone usage shares and TD shares following GPT guidelines",
        "status": "running",
        "endpoints": {
            "/player-usage": {
                "method": "GET",
                "description": "Get all teams player usage data",
                "parameters": {
                    "team": "Optional - get data for specific team (e.g., ?team=KC)"
                }
            },
            "/player-usage/<team>": {
                "method": "GET",
                "description": "Get player usage data for specific team",
                "example": "/player-usage/KC"
            },
            "/refresh": {
                "method": "POST",
                "description": "Manual data refresh"
            },
            "/health": {
                "method": "GET",
                "description": "Health check"
            }
        },
        "methodology": {
            "rz_usage_share": "Player share of team RZ opportunities with 2+ plays filter, no 2-pt conversions",
            "td_share": "Player share of team TDs (rush + receiving)",
            "notes": "Uses player IDs to avoid name collisions, accumulates across position types"
        }
    })

if __name__ == '__main__':
    # Railway provides PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
