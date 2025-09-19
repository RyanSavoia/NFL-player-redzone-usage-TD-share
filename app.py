import nfl_data_py as nfl
import pandas as pd
import json
import os
import logging
from datetime import datetime
from flask import Flask, jsonify, request

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

class PlayerUsageAnalyzer:
    def __init__(self):
        """Initialize the Player Usage Analyzer"""
        self.pbp_data = None
        self.data_loaded = False
        
    def load_data(self):
        """Load NFL play-by-play data"""
        try:
            logger.info("Loading 2025 NFL play-by-play data...")
            self.pbp_data = nfl.import_pbp_data([2025])
            
            if self.pbp_data.empty:
                raise ValueError("No 2025 NFL data available")
            
            self.data_loaded = True
            logger.info(f"Successfully loaded {len(self.pbp_data)} total plays")
            
        except Exception as e:
            logger.error(f"Error loading NFL data: {str(e)}")
            self.pbp_data = pd.DataFrame()
            self.data_loaded = False
            raise
    
    def get_team_rz_usage(self, team):
        """Get red zone usage shares for a team"""
        try:
            if not self.data_loaded or self.pbp_data.empty:
                return {}
            
            # Filter for red zone plays, excluding 2-point conversions
            rz_data = self.pbp_data[
                (self.pbp_data['posteam'] == team) & 
                (self.pbp_data['yardline_100'] <= 20) & 
                ((self.pbp_data['rush'] == 1) | (self.pbp_data['pass'] == 1)) &
                (self.pbp_data['two_point_attempt'] != 1)
            ].copy()
            
            if rz_data.empty:
                return {}
            
            # Apply 2+ plays filter per drive
            filtered_plays = []
            for game_id in rz_data['game_id'].unique():
                if pd.isna(game_id):
                    continue
                game_data = rz_data[rz_data['game_id'] == game_id]
                for drive_id in game_data['fixed_drive'].unique():
                    if pd.isna(drive_id):
                        continue
                    drive_plays = game_data[game_data['fixed_drive'] == drive_id]
                    if len(drive_plays) >= 2:
                        filtered_plays.append(drive_plays)
            
            if not filtered_plays:
                return {}
            
            rz_filtered = pd.concat(filtered_plays, ignore_index=True)
            total_rz_plays = len(rz_filtered)
            usage_shares = {}
            
            # Process rushing plays
            rush_data = rz_filtered[rz_filtered['rush'] == 1]
            if not rush_data.empty:
                rush_counts = rush_data['rusher_player_id'].value_counts()
                for player_id, count in rush_counts.items():
                    if pd.notna(player_id):
                        share = count / total_rz_plays
                        player_name_series = rush_data[rush_data['rusher_player_id'] == player_id]['rusher_player_name']
                        if not player_name_series.empty:
                            player_name = player_name_series.iloc[0]
                            if pd.notna(player_name):
                                usage_shares[player_name] = usage_shares.get(player_name, 0) + share
            
            # Process passing plays
            pass_data = rz_filtered[rz_filtered['pass'] == 1]
            if not pass_data.empty:
                target_counts = pass_data['receiver_player_id'].value_counts()
                for player_id, count in target_counts.items():
                    if pd.notna(player_id):
                        share = count / total_rz_plays
                        player_name_series = pass_data[pass_data['receiver_player_id'] == player_id]['receiver_player_name']
                        if not player_name_series.empty:
                            player_name = player_name_series.iloc[0]
                            if pd.notna(player_name):
                                usage_shares[player_name] = usage_shares.get(player_name, 0) + share
            
            return usage_shares
            
        except Exception as e:
            logger.error(f"Error calculating RZ usage for {team}: {str(e)}")
            return {}
    
    def get_team_td_shares(self, team):
        """Get TD shares for a team"""
        try:
            if not self.data_loaded or self.pbp_data.empty:
                return {}
            
            td_data = self.pbp_data[
                (self.pbp_data['posteam'] == team) & 
                (self.pbp_data['touchdown'] == 1)
            ].copy()
            
            if td_data.empty:
                return {}
            
            total_tds = len(td_data)
            td_shares = {}
            
            # Process rushing TDs
            rush_tds = td_data[td_data['rush'] == 1]
            if not rush_tds.empty:
                rush_td_counts = rush_tds['rusher_player_id'].value_counts()
                for player_id, count in rush_td_counts.items():
                    if pd.notna(player_id):
                        share = count / total_tds
                        player_name_series = rush_tds[rush_tds['rusher_player_id'] == player_id]['rusher_player_name']
                        if not player_name_series.empty:
                            player_name = player_name_series.iloc[0]
                            if pd.notna(player_name):
                                td_shares[player_name] = td_shares.get(player_name, 0) + share
            
            # Process receiving TDs
            pass_tds = td_data[td_data['pass'] == 1]
            if not pass_tds.empty:
                rec_td_counts = pass_tds['receiver_player_id'].value_counts()
                for player_id, count in rec_td_counts.items():
                    if pd.notna(player_id):
                        share = count / total_tds
                        player_name_series = pass_tds[pass_tds['receiver_player_id'] == player_id]['receiver_player_name']
                        if not player_name_series.empty:
                            player_name = player_name_series.iloc[0]
                            if pd.notna(player_name):
                                td_shares[player_name] = td_shares.get(player_name, 0) + share
            
            return td_shares
            
        except Exception as e:
            logger.error(f"Error calculating TD shares for {team}: {str(e)}")
            return {}
    
    def analyze_team(self, team):
        """Analyze a single team's player usage"""
        try:
            logger.info(f"Analyzing player usage for {team}")
            
            rz_usage = self.get_team_rz_usage(team)
            td_shares = self.get_team_td_shares(team)
            
            # Combine all players
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
            
            # Sort by combined usage
            team_data['players'] = dict(sorted(
                team_data['players'].items(),
                key=lambda x: x[1]['rz_usage_share'] + x[1]['td_share'],
                reverse=True
            ))
            
            return team_data
            
        except Exception as e:
            logger.error(f"Error analyzing team {team}: {str(e)}")
            return None
    
    def analyze_all_teams(self):
        """Analyze all NFL teams"""
        try:
            if not self.data_loaded:
                self.load_data()
            
            if not self.data_loaded:
                return None
            
            # Get all teams
            unique_teams = self.pbp_data['posteam'].dropna().unique()
            teams = sorted([team for team in unique_teams if pd.notna(team)])
            
            results = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'teams': {}
            }
            
            for team in teams:
                team_data = self.analyze_team(team)
                if team_data:
                    results['teams'][team] = team_data
            
            return results
            
        except Exception as e:
            logger.error(f"Error in analyze_all_teams: {str(e)}")
            return None

# Global analyzer instance
analyzer = None

def get_analyzer():
    """Get or create analyzer instance"""
    global analyzer
    if analyzer is None:
        analyzer = PlayerUsageAnalyzer()
    return analyzer

def run_analysis(team=None):
    """Run analysis function"""
    try:
        current_analyzer = get_analyzer()
        
        if team:
            return current_analyzer.analyze_team(team)
        else:
            return current_analyzer.analyze_all_teams()
            
    except Exception as e:
        logger.error(f"Analysis execution failed: {str(e)}")
        return None

@app.route('/player-usage', methods=['GET'])
def get_player_usage():
    """Get player usage data"""
    try:
        team = request.args.get('team')
        
        if team:
            result = run_analysis(team.upper())
        else:
            result = run_analysis()
        
        if result is None:
            return jsonify({
                "error": "Analysis failed",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }), 500
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in player usage endpoint: {str(e)}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/player-usage/<team>', methods=['GET'])
def get_team_player_usage(team):
    """Get player usage data for specific team"""
    try:
        result = run_analysis(team.upper())
        
        if result is None:
            return jsonify({
                "error": f"Analysis failed for {team}",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }), 500
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in team endpoint for {team}: {str(e)}")
        return jsonify({
            "error": str(e),
            "team": team,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        current_analyzer = get_analyzer()
        return jsonify({
            "status": "healthy",
            "service": "Player Usage Service",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "data_loaded": current_analyzer.data_loaded if current_analyzer else False
        })
    except Exception as e:
        return jsonify({
            "status": "ready",
            "service": "Player Usage Service",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "error": str(e)
        })

@app.route('/', methods=['GET'])
def root():
    """API documentation"""
    return jsonify({
        "service": "Player Usage Service",
        "description": "NFL player red zone usage and TD share analysis",
        "endpoints": {
            "/player-usage": "Get all teams (or ?team=KC for specific team)",
            "/player-usage/<team>": "Get specific team data",
            "/health": "Health check"
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
