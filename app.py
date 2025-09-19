import nfl_data_py as nfl
import pandas as pd
import json
import os
import logging
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

class PlayerUsageService:
    def __init__(self):
        """Initialize the service"""
        self.pbp_data = None
        self.data_loaded = False
        logger.info("PlayerUsageService initialized")
        
    def load_data(self):
        """Load NFL data with robust error handling"""
        if self.data_loaded:
            return True
            
        try:
            logger.info("Loading 2025 NFL data...")
            
            # Try to load 2025 data
            try:
                self.pbp_data = nfl.import_pbp_data([2025])
                logger.info("Successfully loaded 2025 data from nfl_data_py")
            except Exception as e:
                logger.error(f"Error loading 2025 NFL data: {str(e)}")
                # Initialize with empty DataFrame on error
                self.pbp_data = pd.DataFrame()
            
            # Handle empty data gracefully
            if self.pbp_data.empty:
                logger.warning("No 2025 data available or data is empty, using empty dataset")
                self.pbp_data = pd.DataFrame()
            else:
                logger.info(f"Successfully loaded {len(self.pbp_data)} plays from 2025 season")
            
            self.data_loaded = True
            return True
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            self.pbp_data = pd.DataFrame()
            self.data_loaded = True  # Mark as loaded even with empty data to prevent infinite retries
            return True
    
    def get_team_usage(self, team):
        """Get usage data for a team"""
        if not self.load_data():
            return {"error": "Data not available"}
            
        try:
            # Handle empty data
            if self.pbp_data.empty:
                return {
                    'team': team,
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_plays': 0,
                    'players': {},
                    'note': 'No 2025 data available yet'
                }
            
            # Check if required columns exist
            if 'posteam' not in self.pbp_data.columns:
                return {
                    'team': team,
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'error': 'Required columns not found in data',
                    'available_columns': list(self.pbp_data.columns)[:10]  # First 10 for debugging
                }
            
            # Filter for team plays
            team_plays = self.pbp_data[self.pbp_data['posteam'] == team]
            
            result = {
                'team': team,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_plays': len(team_plays),
                'players': {}
            }
            
            # If we have data, do some basic analysis
            if not team_plays.empty:
                # Analyze passing plays if available
                if 'pass' in team_plays.columns and 'receiver_player_name' in team_plays.columns:
                    passing_plays = team_plays[(team_plays['pass'] == 1) & 
                                             (team_plays['receiver_player_name'].notna())]
                    
                    if not passing_plays.empty:
                        receiver_targets = passing_plays['receiver_player_name'].value_counts()
                        result['players']['receivers'] = receiver_targets.head(10).to_dict()
                
                # Analyze rushing plays if available
                if 'rush' in team_plays.columns and 'rusher_player_name' in team_plays.columns:
                    rushing_plays = team_plays[(team_plays['rush'] == 1) & 
                                             (team_plays['rusher_player_name'].notna())]
                    
                    if not rushing_plays.empty:
                        rusher_attempts = rushing_plays['rusher_player_name'].value_counts()
                        result['players']['rushers'] = rusher_attempts.head(10).to_dict()
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing {team}: {str(e)}")
            return {
                "error": str(e),
                "team": team,
                "analysis_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def get_all_teams(self):
        """Get data for all teams"""
        if not self.load_data():
            return {"error": "Data not available", "teams": {}}
            
        try:
            # Handle empty data
            if self.pbp_data.empty:
                logger.info("Empty dataset, returning empty teams")
                return {
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'teams': {},
                    'note': 'No 2025 data available yet'
                }
            
            # Check if posteam column exists
            if 'posteam' not in self.pbp_data.columns:
                logger.warning("No 'posteam' column found in data")
                return {
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'teams': {},
                    'error': 'Required columns not found',
                    'available_columns': list(self.pbp_data.columns)[:10]  # First 10 columns for debugging
                }
            
            # Get unique teams
            teams = self.pbp_data['posteam'].dropna().unique()
            
            result = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_teams': len(teams),
                'teams': {}
            }
            
            # Build team summary
            for team in sorted(teams):
                team_plays = self.pbp_data[self.pbp_data['posteam'] == team]
                result['teams'][team] = {
                    'team': team,
                    'total_plays': len(team_plays),
                    'status': 'available'
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting all teams: {str(e)}")
            return {
                "error": str(e), 
                "teams": {},
                "analysis_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "data_shape": self.pbp_data.shape if hasattr(self, 'pbp_data') and self.pbp_data is not None else "No data"
            }

# Global service instance
service = PlayerUsageService()

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "Player Usage Service",
        "status": "running",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "endpoints": {
            "/health": "Health check",
            "/player-usage": "Get all teams or specific team usage (use ?team=TEAM)",
            "/player-usage/<team>": "Get specific team usage"
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "service": "Player Usage Service",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "data_loaded": service.data_loaded,
        "data_available": not service.pbp_data.empty if service.pbp_data is not None else False
    })

@app.route('/player-usage', methods=['GET'])
def player_usage():
    """Player usage endpoint"""
    try:
        team = request.args.get('team')
        
        if team:
            result = service.get_team_usage(team.upper())
        else:
            result = service.get_all_teams()
            
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Endpoint error: {str(e)}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/player-usage/<team>', methods=['GET'])
def team_usage(team):
    """Team specific endpoint"""
    try:
        result = service.get_team_usage(team.upper())
        return jsonify(result)
    except Exception as e:
        logger.error(f"Team endpoint error: {str(e)}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }), 500

# Initialize data loading on startup (non-blocking)
@app.before_first_request
def initialize_service():
    """Initialize service before first request"""
    try:
        logger.info("Initializing service on startup...")
        service.load_data()
    except Exception as e:
        logger.error(f"Startup initialization error: {str(e)}")

if __name__ == '__main__':
    # Use port 10000 to match working tool
    port = int(os.environ.get('PORT', 10000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting Player Usage Service on port {port}")
    
    # In production, gunicorn will handle this
    # In development, Flask dev server will run
    app.run(host='0.0.0.0', port=port, debug=debug)
