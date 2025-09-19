import nfl_data_py as nfl
import pandas as pd
import json
from datetime import datetime, timedelta
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlayerUsageAnalyzer:
    def __init__(self, performance_year=2025):
        """
        Initialize Player Usage Analyzer with the same pattern as working tools
        """
        # Explicitly prevent 2024 data usage
        if performance_year == 2024:
            raise ValueError("2024 data is not allowed - this tool only works with 2025+ data")
        
        self.performance_year = performance_year
        self.pbp_data = None
        self.data_loaded = False
        
        try:
            self.load_data()
            self.data_loaded = True
            logger.info(f"Successfully initialized analyzer: {performance_year} player usage analysis")
        except Exception as e:
            logger.error(f"Failed to initialize analyzer: {str(e)}")
            self.data_loaded = False
    
    def load_data(self):
        """Load NFL data with error handling"""
        try:
            # Double-check we're not loading 2024 data
            if self.performance_year == 2024:
                raise ValueError("2024 data is explicitly blocked - this tool only works with 2025+ data")
            
            logger.info(f"Loading {self.performance_year} NFL play-by-play data...")
            self.pbp_data = nfl.import_pbp_data([self.performance_year])
            
            if self.pbp_data.empty:
                raise ValueError(f"No data available for {self.performance_year}")
            
            # Additional safeguard: verify loaded data is from correct year
            if 'season' in self.pbp_data.columns:
                loaded_years = self.pbp_data['season'].unique()
                if 2024 in loaded_years:
                    raise ValueError("Detected 2024 data in loaded dataset - this is not allowed")
                if self.performance_year not in loaded_years:
                    raise ValueError(f"Loaded data does not contain {self.performance_year} season data")
                
            logger.info(f"Data loaded successfully: {len(self.pbp_data)} total plays")
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise
    
    def get_player_rz_usage_share(self, team):
        """
        Get player red zone usage shares with 2+ plays filter and no 2-pt conversions
        Following exact specifications from the local tool
        """
        try:
            if not self.data_loaded:
                self.load_data()
            
            # Filter for red zone plays, excluding 2-point conversions
            rz_data = self.pbp_data[
                (self.pbp_data['posteam'] == team) & 
                (self.pbp_data['yardline_100'] <= 20) & 
                ((self.pbp_data['rush'] == 1) | (self.pbp_data['pass'] == 1)) &
                (self.pbp_data['two_point_attempt'] != 1)  # Exclude 2-pt attempts
            ].copy()
            
            if rz_data.empty:
                return {}
            
            # Apply 2+ plays filter per drive (exact requirement)
            filtered_plays = []
            for game_id in rz_data['game_id'].unique():
                game_data = rz_data[rz_data['game_id'] == game_id]
                for drive_id in game_data['fixed_drive'].unique():
                    drive_plays = game_data[game_data['fixed_drive'] == drive_id]
                    if len(drive_plays) >= 2:  # 2+ plays filter
                        filtered_plays.append(drive_plays)
            
            if not filtered_plays:
                return {}
            
            rz_filtered = pd.concat(filtered_plays, ignore_index=True)
            total_rz_plays = len(rz_filtered)
            usage_shares = {}
            
            # Accumulate rushing usage by player_id then add names
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
            
        except Exception as e:
            logger.error(f"Error analyzing RZ usage for {team}: {str(e)}")
            return {}
    
    def get_player_td_share(self, team):
        """Get player TD shares (accumulating rush + receiving TDs)"""
        try:
            if not self.data_loaded:
                self.load_data()
            
            td_data = self.pbp_data[
                (self.pbp_data['posteam'] == team) & 
                (self.pbp_data['touchdown'] == 1)
            ].copy()
            
            if td_data.empty:
                return {}
            
            total_tds = len(td_data)
            td_shares = {}
            
            # Accumulate rushing TDs by player_id then add names
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
            
        except Exception as e:
            logger.error(f"Error analyzing TD shares for {team}: {str(e)}")
            return {}
    
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
            
            return team_data
            
        except Exception as e:
            logger.error(f"Error getting team player usage for {team}: {str(e)}")
            return None
    
    def get_all_teams_usage(self):
        """Get player usage data for all 32 NFL teams"""
        if not self.data_loaded:
            logger.error("Analyzer not properly initialized")
            return {"error": "Data not loaded"}
        
        try:
            # Get all unique teams from current season
            teams = sorted(self.pbp_data['posteam'].dropna().unique())
            
            all_teams_data = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'performance_year': self.performance_year,
                'methodology': {
                    'rz_usage_share': 'Player share of team red zone opportunities (rush attempts + targets) with 2+ plays filter, excluding 2-pt conversions',
                    'td_share': 'Player share of team touchdowns (rushing + receiving)',
                    'calculation': 'Uses player IDs to avoid name collisions, accumulates across rush and receiving'
                },
                'teams': {}
            }
            
            for team in teams:
                logger.info(f"Processing {team}...")
                team_data = self.get_team_player_usage(team)
                if team_data:
                    all_teams_data['teams'][team] = team_data
            
            logger.info(f"Completed analysis for {len(teams)} teams")
            return all_teams_data
            
        except Exception as e:
            logger.error(f"Error getting all teams usage: {str(e)}")
            return {"error": f"Analysis failed: {str(e)}"}
    
    def generate_json_output(self, results, include_metadata=True):
        """Generate JSON output for API consumption"""
        try:
            output = {
                "results": results,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "performance_year": self.performance_year,
                    "data_loaded": self.data_loaded,
                    "disclaimer": "For educational analysis only. Player usage calculated with 2+ plays filter for red zone drives."
                } if include_metadata else None
            }
            
            if not include_metadata:
                output = {"results": results}
                
            return json.dumps(output, indent=2)
            
        except Exception as e:
            logger.error(f"Error generating JSON output: {str(e)}")
            return json.dumps({"error": "Failed to generate output", "message": str(e)})

def run_analysis():
    """Run analysis function - separates logic from main() for flask integration"""
    try:
        # Configuration - can be set via environment variables
        performance_year = int(os.getenv('PERFORMANCE_YEAR', 2025))
        target_team = os.getenv('TARGET_TEAM')  # Optional specific team
        
        analyzer = PlayerUsageAnalyzer(performance_year=performance_year)
        
        if not analyzer.data_loaded:
            logger.error("Failed to load data")
            return None
        
        # Analyze teams
        if target_team:
            results = analyzer.get_team_player_usage(target_team.upper())
        else:
            results = analyzer.get_all_teams_usage()
        
        if not results:
            logger.error("No valid results found")
            return None
        
        return {
            'results': results,
            'analyzer': analyzer
        }
        
    except Exception as e:
        logger.error(f"Analysis execution failed: {str(e)}")
        return None

def main():
    """Main function for command-line execution"""
    analysis_data = run_analysis()
    
    if not analysis_data:
        sys.exit(1)
    
    results = analysis_data['results']
    analyzer = analysis_data['analyzer']
    
    # Output JSON for API consumption
    json_output = analyzer.generate_json_output(results)
    print(json_output)
    
    # Human-readable summary to stderr for logging
    print(f"\n=== NFL PLAYER USAGE ANALYSIS ===", file=sys.stderr)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)
    print("="*50, file=sys.stderr)
    
    return results

# Flask Integration
try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes
    
    @app.route('/player-usage', methods=['GET'])
    def get_player_usage():
        """Get player usage data for all teams or specific team"""
        try:
            team = request.args.get('team')  # Optional team parameter
            performance_year = request.args.get('performance_year', '2025')
            
            # Set environment variables temporarily
            original_env = {}
            env_vars = {
                'PERFORMANCE_YEAR': performance_year
            }
            if team:
                env_vars['TARGET_TEAM'] = team.upper()
            
            # Store original values and set new ones
            for key, value in env_vars.items():
                original_env[key] = os.environ.get(key)
                os.environ[key] = value
            
            try:
                # Run analysis
                analysis_data = run_analysis()
                
                if not analysis_data:
                    return jsonify({
                        "error": "Analysis failed",
                        "message": "Could not complete analysis",
                        "timestamp": datetime.now().isoformat()
                    }), 500
                
                # Return results
                results = analysis_data['results']
                return jsonify(results)
                
            finally:
                # Restore original environment variables
                for key, original_value in original_env.items():
                    if original_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_value
        
        except Exception as e:
            logger.error(f"Error in Flask endpoint: {str(e)}")
            return jsonify({
                "error": "Unexpected error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }), 500
    
    @app.route('/player-usage/<team>', methods=['GET'])
    def get_team_player_usage_endpoint(team):
        """Get player usage data for a specific team"""
        try:
            performance_year = request.args.get('performance_year', '2025')
            
            # Set environment variables temporarily
            original_env = {}
            env_vars = {
                'PERFORMANCE_YEAR': performance_year,
                'TARGET_TEAM': team.upper()
            }
            
            # Store original values and set new ones
            for key, value in env_vars.items():
                original_env[key] = os.environ.get(key)
                os.environ[key] = value
            
            try:
                # Run analysis
                analysis_data = run_analysis()
                
                if not analysis_data:
                    return jsonify({
                        "error": "Analysis failed",
                        "message": f"Could not analyze team {team}",
                        "timestamp": datetime.now().isoformat()
                    }), 500
                
                # Return results
                results = analysis_data['results']
                return jsonify(results)
                
            finally:
                # Restore original environment variables
                for key, original_value in original_env.items():
                    if original_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_value
        
        except Exception as e:
            logger.error(f"Error in team Flask endpoint: {str(e)}")
            return jsonify({
                "error": "Unexpected error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }), 500
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint"""
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "Player Usage Analyzer"
        })
    
    @app.route('/', methods=['GET'])
    def root():
        """Root endpoint with API documentation"""
        return jsonify({
            "service": "Player Usage Analyzer API",
            "version": "1.0",
            "endpoints": {
                "/player-usage": {
                    "method": "GET",
                    "description": "Get all teams player usage data",
                    "parameters": {
                        "team": "Optional - get data for specific team (e.g., ?team=KC)",
                        "performance_year": "Year to analyze (default: 2025)"
                    },
                    "example": "/player-usage?team=KC&performance_year=2025"
                },
                "/player-usage/<team>": {
                    "method": "GET",
                    "description": "Get player usage data for specific team",
                    "parameters": {
                        "performance_year": "Year to analyze (default: 2025)"
                    },
                    "example": "/player-usage/KC?performance_year=2025"
                },
                "/health": {
                    "method": "GET", 
                    "description": "Health check endpoint"
                }
            },
            "methodology": {
                "rz_usage_share": "Player share of team RZ opportunities with 2+ plays filter, no 2-pt conversions",
                "td_share": "Player share of team TDs (rush + receiving)",
                "notes": "Uses player IDs to avoid name collisions, accumulates across position types"
            },
            "timestamp": datetime.now().isoformat()
        })
    
    def run_flask_app():
        """Run Flask application"""
        port = int(os.environ.get('PORT', 10000))
        debug = os.environ.get('DEBUG', 'False').lower() == 'true'
        
        logger.info(f"Starting Player Usage Analyzer API on port {port}")
        app.run(host='0.0.0.0', port=port, debug=debug)

except ImportError:
    logger.info("Flask not available - running in CLI mode only")
    app = None
    def run_flask_app():
        logger.error("Flask not installed. Install with: pip install flask")
        sys.exit(1)

if __name__ == "__main__":
    # Check if Flask mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == '--flask':
        if app is not None:
            run_flask_app()
        else:
            logger.error("Flask not available. Install with: pip install flask")
            sys.exit(1)
    else:
        main()
