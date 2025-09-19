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

class PlayerUsageService:
    def __init__(self):
        """Initialize the service"""
        self.pbp_data = None
        self.data_loaded = False
        logger.info("PlayerUsageService initialized")
        
    def load_data(self):
        """Load NFL data"""
        if self.data_loaded:
            return True
            
        try:
            logger.info("Loading 2025 NFL data...")
            
            def load_2025_data():
                try:
                    return nfl.import_pbp_data([2025])
                except Exception as e:
                    logger.error(f"Error loading 2025 NFL data: {str(e)}")
                    # Return empty DataFrame on error
                    return pd.DataFrame()
            
            self.pbp_data = load_2025_data()
            
            # Handle empty data gracefully like your working code
            if self.pbp_data.empty:
                logger.info("No 2025 data available, using empty dataset")
                self.pbp_data = pd.DataFrame()
                self.data_loaded = True  # Still mark as loaded
                return True
                
            self.data_loaded = True
            logger.info(f"Successfully loaded {len(self.pbp_data)} plays")
            return True
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            self.pbp_data = pd.DataFrame()
            self.data_loaded = True  # Mark as loaded even with empty data
            return True
    
    def get_team_usage(self, team):
        """Get usage data for a team"""
        if not self.load_data():
            return {"error": "Data not available"}
            
        try:
            # Simple test - just count plays for this team
            team_plays = self.pbp_data[self.pbp_data['posteam'] == team]
            
            result = {
                'team': team,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_plays': len(team_plays),
                'players': {}
            }
            
            # Just return basic structure for now
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing {team}: {str(e)}")
            return {"error": str(e)}
    
    def get_all_teams(self):
        """Get data for all teams"""
        if not self.load_data():
            return {"error": "Data not available", "teams": {}}
            
        try:
            teams = self.pbp_data['posteam'].dropna().unique()
            
            result = {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_teams': len(teams),
                'teams': {}
            }
            
            # Just return team list for now
            for team in sorted(teams):
                result['teams'][team] = {
                    'team': team,
                    'status': 'available'
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting all teams: {str(e)}")
            return {"error": str(e), "teams": {}}

# Global service
service = PlayerUsageService()

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "service": "Player Usage Service",
        "status": "running",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "service": "Player Usage Service",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "data_loaded": service.data_loaded
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
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
