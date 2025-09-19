from flask import Flask, jsonify
import requests
import json
import time
from datetime import datetime
import nfl_data_py as nfl
import pandas as pd

app = Flask(__name__)

def timed_operation(description, func):
    """Helper function to time operations and log them"""
    print(f"Starting {description}...")
    start_time = time.time()
    try:
        result = func()
        duration = time.time() - start_time
        print(f"{description} completed in {duration:.2f} seconds")
        return result
    except Exception as e:
        duration = time.time() - start_time
        print(f"{description} failed after {duration:.2f} seconds: {str(e)}")
        raise

class TeamAnalysisService:
    def __init__(self, odds_api_key="d8ba5d45eca27e710d7ef2680d8cb452"):
        """Combines Vegas team totals with TD boost calculations"""
        self.odds_api_key = odds_api_key
        
        # Hardcoded 2024 league averages (never change, massive startup speed improvement)
        self.league_averages_2024 = {
            'rz_scoring': 59.0,
            'rz_allow': 59.0, 
            'all_drives_scoring': 23.3,
            'all_drives_allow': 23.3
        }
        
        # Team name mapping: Full Name -> Abbreviation
        self.team_mapping = {
            "Arizona Cardinals": "ARI",
            "Atlanta Falcons": "ATL", 
            "Baltimore Ravens": "BAL",
            "Buffalo Bills": "BUF",
            "Carolina Panthers": "CAR",
            "Chicago Bears": "CHI",
            "Cincinnati Bengals": "CIN",
            "Cleveland Browns": "CLE",
            "Dallas Cowboys": "DAL",
            "Denver Broncos": "DEN",
            "Detroit Lions": "DET",
            "Green Bay Packers": "GB",
            "Houston Texans": "HOU",
            "Indianapolis Colts": "IND",
            "Jacksonville Jaguars": "JAX",
            "Kansas City Chiefs": "KC",
            "Los Angeles Rams": "LAR",
            "Miami Dolphins": "MIA",
            "Minnesota Vikings": "MIN",
            "New England Patriots": "NE",
            "New Orleans Saints": "NO",
            "New York Giants": "NYG",
            "New York Jets": "NYJ",
            "Las Vegas Raiders": "LV",
            "Philadelphia Eagles": "PHI",
            "Pittsburgh Steelers": "PIT",
            "Los Angeles Chargers": "LAC",
            "San Francisco 49ers": "SF",
            "Seattle Seahawks": "SEA",
            "Tampa Bay Buccaneers": "TB",
            "Tennessee Titans": "TEN",
            "Washington Commanders": "WAS"
        }
        
        # Bookmaker priority
        self.book_priority = ['fanduel', 'draftkings', 'betmgm', 'caesars', 'betrivers']
        
        # Initialize TD boost calculator after class is defined
        self.td_calculator = None
    
    def _ensure_calculator_initialized(self):
        """Initialize the TD calculator on first use"""
        if self.td_calculator is None:
            self.td_calculator = NFLTDBoostCalculator(service_instance=self)
    
    def get_week_parameters(self, week=None):
        """Get consistent edge weight for all season"""
        # Constant 25% weighting to your TD advantage throughout season
        w_edge = 0.25
        return w_edge
    
    def get_current_week(self):
        """Get current NFL week"""
        try:
            df_2025 = nfl.import_pbp_data([2025])
            if not df_2025.empty:
                max_week = df_2025['week'].max()
                return int(max_week) + 1
            return 3
        except Exception as e:
            print(f"Error getting current week: {str(e)}")
            return 3
    
    def get_vegas_team_totals(self):
        """Get Vegas-implied team TD totals, filtered to current week games"""
        self._ensure_calculator_initialized()
        
        url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds?regions=us&markets=totals,spreads&oddsFormat=american&apiKey={self.odds_api_key}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            games_data = response.json()
        except Exception as e:
            print(f"Error fetching odds data: {e}")
            return {}
        
        # Get current week matchups to filter games
        current_week_matchups = self.td_calculator.get_week_matchups()
        if not current_week_matchups:
            return {}
        
        # Create expected games set for filtering
        expected_games = set()
        for matchup in current_week_matchups:
            expected_games.add(f"{matchup['away_team']}@{matchup['home_team']}")
        
        vegas_totals = {}
        
        for game in games_data:
            home_team = game['home_team']
            away_team = game['away_team']
            
            # Map to abbreviations
            home_abbr = self.team_mapping.get(home_team)
            away_abbr = self.team_mapping.get(away_team)
            
            if not home_abbr or not away_abbr:
                continue
            
            game_key = f"{away_abbr}@{home_abbr}"
            
            # Skip games not in current week
            if game_key not in expected_games:
                continue
            
            # Get bookmaker data
            selected_bookmaker = None
            for book_key in self.book_priority:
                for bookmaker in game['bookmakers']:
                    if bookmaker['key'] == book_key:
                        selected_bookmaker = bookmaker
                        break
                if selected_bookmaker:
                    break
            
            if not selected_bookmaker:
                continue
            
            # Extract totals and spreads
            totals_market = None
            spreads_market = None
            
            for market in selected_bookmaker['markets']:
                if market['key'] == 'totals':
                    totals_market = market
                elif market['key'] == 'spreads':
                    spreads_market = market
            
            if not totals_market or not spreads_market:
                continue
            
            # Get game total
            game_total = None
            for outcome in totals_market['outcomes']:
                if outcome['name'] == 'Over':
                    game_total = outcome['point']
                    break
            
            if game_total is None:
                game_total = totals_market['outcomes'][0]['point']
            
            # Get spreads
            home_spread = None
            away_spread = None
            
            for outcome in spreads_market['outcomes']:
                if outcome['name'] == home_team:
                    home_spread = outcome['point']
                elif outcome['name'] == away_team:
                    away_spread = outcome['point']
            
            if home_spread is None or away_spread is None:
                continue
            
            # Calculate implied points
            if home_spread < 0:  # Home team favored
                home_implied_points = (game_total - home_spread) / 2
                away_implied_points = (game_total + home_spread) / 2
            else:  # Away team favored
                home_implied_points = (game_total + home_spread) / 2
                away_implied_points = (game_total - home_spread) / 2
            
            # Apply 25% reduction for field goals (75% of points come from TDs)
            fg_penalty = 0.75
            home_td_points = home_implied_points * fg_penalty
            away_td_points = away_implied_points * fg_penalty
            
            # Convert to TDs (7 points per TD)
            home_vegas_tds = round(home_td_points / 7, 2)
            away_vegas_tds = round(away_td_points / 7, 2)
            
            vegas_totals[game_key] = {
                'home_team': home_abbr,
                'away_team': away_abbr,
                'home_vegas_tds': home_vegas_tds,
                'away_vegas_tds': away_vegas_tds,
                'commence_time': game['commence_time'],
                'bookmaker': selected_bookmaker['key']
            }
        
        return vegas_totals
    
    def get_team_analysis(self, week=None):
        """
        Combine Vegas team totals with TD boost advantages
        Following GPT's exact formula: team_td_proj = vegas_team_tds * (1 + w_edge * advantage_pct)
        """
        try:
            self._ensure_calculator_initialized()
            
            # Get Vegas totals
            vegas_totals = self.get_vegas_team_totals()
            if not vegas_totals:
                return {"error": "No Vegas data available"}
            
            # Get week parameters
            w_edge = self.get_week_parameters(week)
            
            # Get TD boost data for all current week games
            td_boost_results = self.td_calculator.analyze_week_matchups(week)
            if 'games' not in td_boost_results:
                return {"error": "No TD boost data available", "details": td_boost_results}
            
            # Combine Vegas totals with TD advantages
            combined_results = {
                'week': week or self.get_current_week(),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'w_edge': w_edge,
                'games': []
            }
            
            for game_data in td_boost_results['games']:
                away_team = game_data['away_team']
                home_team = game_data['home_team']
                game_key = f"{away_team}@{home_team}"
                
                if game_key not in vegas_totals:
                    continue
                
                vegas_game = vegas_totals[game_key]
                
                # Get TD advantages (convert from percentage to decimal)
                away_advantage_raw = game_data['away_offense_vs_home_defense']['combined_team_analysis'].get('total_team_td_advantage_pct', 0)
                home_advantage_raw = game_data['home_offense_vs_away_defense']['combined_team_analysis'].get('total_team_td_advantage_pct', 0)
                
                if away_advantage_raw is None:
                    away_advantage_raw = 0
                if home_advantage_raw is None:
                    home_advantage_raw = 0
                
                # Convert to decimal and cap at ±30% (GPT's formula)
                away_advantage_pct = max(-0.30, min(0.30, away_advantage_raw / 100))
                home_advantage_pct = max(-0.30, min(0.30, home_advantage_raw / 100))
                
                # Apply GPT's formula: team_td_proj = vegas_team_tds * (1 + w_edge * advantage_pct)
                away_projected_tds = vegas_game['away_vegas_tds'] * (1 + w_edge * away_advantage_pct)
                home_projected_tds = vegas_game['home_vegas_tds'] * (1 + w_edge * home_advantage_pct)
                
                combined_game = {
                    'game': f"{away_team} @ {home_team}",
                    'commence_time': vegas_game['commence_time'],
                    'bookmaker': vegas_game['bookmaker'],
                    'away_team': away_team,
                    'home_team': home_team,
                    
                    # Vegas baseline
                    'away_vegas_tds': vegas_game['away_vegas_tds'],
                    'home_vegas_tds': vegas_game['home_vegas_tds'],
                    
                    # TD advantages
                    'away_td_advantage_pct': round(away_advantage_raw, 1),
                    'home_td_advantage_pct': round(home_advantage_raw, 1),
                    
                    # Final projected TDs (GPT's formula applied)
                    'away_projected_tds': round(away_projected_tds, 2),
                    'home_projected_tds': round(home_projected_tds, 2),
                    
                    # Show the calculation
                    'calculation': {
                        'w_edge': w_edge,
                        'away_calc': f"{vegas_game['away_vegas_tds']} * (1 + {w_edge} * {away_advantage_pct:.3f}) = {away_projected_tds:.2f}",
                        'home_calc': f"{vegas_game['home_vegas_tds']} * (1 + {w_edge} * {home_advantage_pct:.3f}) = {home_projected_tds:.2f}"
                    }
                }
                
                combined_results['games'].append(combined_game)
            
            return combined_results
            
        except Exception as e:
            print(f"Error in get_team_analysis: {str(e)}")
            return {"error": f"Analysis failed: {str(e)}"}

    def refresh_data(self):
        """Refresh data method for manual refresh endpoint"""
        try:
            if self.td_calculator:
                self.td_calculator.load_data()
            return True
        except Exception as e:
            print(f"Error refreshing data: {str(e)}")
            raise

class NFLTDBoostCalculator:
    def __init__(self, service_instance=None):
        """Initialize the TD Boost Calculator with consistent methodology"""
        self.service_instance = service_instance
        self.current_2025 = {}
        self.schedule_data = None
        self.league_averages = {}
        self.data_loaded = False
        
        # Initialize data on startup
        try:
            self.load_data()
            self.data_loaded = True
            print("NFLTDBoostCalculator initialized successfully")
        except Exception as e:
            print(f"Failed to initialize NFLTDBoostCalculator: {str(e)}")
            self.data_loaded = False
        
    def load_schedule(self):
        """Load NFL schedule data"""
        try:
            print("Loading 2025 NFL schedule...")
            self.schedule_data = nfl.import_schedules([2025])
            
            if self.schedule_data.empty:
                raise ValueError("No schedule data available for 2025")
            
            # Convert game_id to ensure proper formatting
            self.schedule_data['gameday'] = pd.to_datetime(self.schedule_data['gameday'])
            print(f"Schedule loaded: {len(self.schedule_data)} games")
            return True
            
        except Exception as e:
            print(f"Failed to load schedule: {str(e)}")
            self.schedule_data = None
            return False
        
    def calculate_rz_stats_with_filter(self, df, year_label=""):
        """Calculate red zone stats with 2+ plays filter for consistent methodology"""
        print(f"Calculating {year_label} red zone stats with 2+ plays filter...")
        
        # Filter for regular season only
        if 'week' in df.columns:
            reg_season = df[df['week'] <= 18] if year_label == "2024" else df
        else:
            reg_season = df
            
        rz_drives = reg_season[(reg_season['yardline_100'] <= 20) & (reg_season['fixed_drive'].notna())]
        
        # Offensive stats
        offense_results = {}
        for team in rz_drives['posteam'].unique():
            if pd.isna(team):
                continue
            team_rz = rz_drives[rz_drives['posteam'] == team]
            play_counts = team_rz.groupby(['game_id', 'fixed_drive']).size()
            multi_play_drives = play_counts[play_counts > 1]  # 2+ plays filter
            
            if len(multi_play_drives) > 0:
                filtered_drives = team_rz[team_rz.set_index(['game_id', 'fixed_drive']).index.isin(multi_play_drives.index)]
                drive_summary = filtered_drives.groupby(['game_id', 'posteam', 'fixed_drive']).agg({'touchdown': 'max'}).reset_index()
                
                drives = len(drive_summary)
                tds = float(drive_summary['touchdown'].sum())
                rate = round(tds/drives*100, 1) if drives > 0 else 0
                offense_results[team] = {
                    'rz_drives': drives,
                    'rz_tds': tds,
                    'rz_td_rate': float(rate)
                }
        
        # Defensive stats
        defense_results = {}
        for team in rz_drives['defteam'].unique():
            if pd.isna(team):
                continue
            team_rz = rz_drives[rz_drives['defteam'] == team]
            play_counts = team_rz.groupby(['game_id', 'fixed_drive']).size()
            multi_play_drives = play_counts[play_counts > 1]  # Same 2+ plays filter
            
            if len(multi_play_drives) > 0:
                filtered_drives = team_rz[team_rz.set_index(['game_id', 'fixed_drive']).index.isin(multi_play_drives.index)]
                drive_summary = filtered_drives.groupby(['game_id', 'defteam', 'fixed_drive']).agg({'touchdown': 'max'}).reset_index()
                
                drives = len(drive_summary)
                tds = float(drive_summary['touchdown'].sum())
                rate = round(tds/drives*100, 1) if drives > 0 else 0
                defense_results[team] = {
                    'rz_drives_faced': drives,
                    'rz_tds_allowed': tds,
                    'rz_td_allow_rate': float(rate)
                }
        
        return offense_results, defense_results
    
    def calculate_all_drives_stats(self, df, year_label=""):
        """Calculate all drives TD stats"""
        print(f"Calculating {year_label} all drives stats...")
        
        # Filter for regular season only
        if 'week' in df.columns:
            reg_season = df[df['week'] <= 18] if year_label == "2024" else df
        else:
            reg_season = df
        
        all_drives = reg_season.groupby(['game_id', 'posteam', 'fixed_drive']).agg({'touchdown': 'max'}).reset_index()
        
        # Offensive stats
        offense_all = all_drives.groupby('posteam').apply(
            lambda x: {
                'total_drives': len(x),
                'total_tds': float(x['touchdown'].sum()),
                'total_td_rate': round(float(x['touchdown'].sum()) / len(x) * 100, 1)
            }, include_groups=False
        ).to_dict()
        
        # Defensive stats  
        all_drives_def = reg_season.groupby(['game_id', 'defteam', 'fixed_drive']).agg({'touchdown': 'max'}).reset_index()
        defense_all = all_drives_def.groupby('defteam').apply(
            lambda x: {
                'total_drives_faced': len(x),
                'total_tds_allowed': float(x['touchdown'].sum()),
                'total_td_allow_rate': round(float(x['touchdown'].sum()) / len(x) * 100, 1)
            }, include_groups=False
        ).to_dict()
        
        return offense_all, defense_all
    
    def calculate_league_averages(self):
        """Use hardcoded 2024 league averages instead of calculating them"""
        self.league_averages = self.service_instance.league_averages_2024.copy()
        print(f"Using hardcoded league averages - RZ scoring: {self.league_averages['rz_scoring']}%, RZ allow: {self.league_averages['rz_allow']}%")
        print(f"All drives - Scoring: {self.league_averages['all_drives_scoring']}%, Allow: {self.league_averages['all_drives_allow']}%")
        
    def load_data(self):
        """Load only 2025 current data - use hardcoded 2024 baselines"""
        try:
            # Use hardcoded league averages (no 2024 data loading needed)
            self.calculate_league_averages()
            
            # Only load 2025 current data (much faster)
            print("Loading 2025 current data...")
            start_time = time.time()
            
            def load_2025_data():
                return nfl.import_pbp_data([2025])
            
            df_2025 = timed_operation("2025 NFL data download", load_2025_data)
            
            def calculate_rz_stats():
                return self.calculate_rz_stats_with_filter(df_2025, "2025")
            
            def calculate_all_drives():
                return self.calculate_all_drives_stats(df_2025, "2025")
            
            off_rz_2025, def_rz_2025 = timed_operation("2025 RZ stats calculation", calculate_rz_stats)
            off_all_2025, def_all_2025 = timed_operation("2025 all drives calculation", calculate_all_drives)
            
            self.current_2025 = {
                'offense_rz': off_rz_2025,
                'defense_rz': def_rz_2025,
                'offense_all': off_all_2025,
                'defense_all': def_all_2025
            }
            
            # Debug output to verify data was loaded
            print(f"2025 data loaded successfully:")
            print(f"  Offense RZ teams: {len(self.current_2025['offense_rz'])}")
            print(f"  Defense RZ teams: {len(self.current_2025['defense_rz'])}")
            print(f"  Sample offense data: {list(self.current_2025['offense_rz'].keys())[:5]}")
            
            # Load schedule
            def load_sched():
                return self.load_schedule()
            
            timed_operation("Schedule data loading", load_sched)
            
            print(f"Total 2025 data loading completed in {time.time() - start_time:.2f} seconds")
            print("Data loading complete (using hardcoded 2024 baselines)!")
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise
    
    def get_current_week(self):
        """Determine current NFL week based on date and available data"""
        try:
            # Get current play-by-play data to see what's been completed
            try:
                df_2025 = nfl.import_pbp_data([2025])
                if not df_2025.empty:
                    max_completed_week = df_2025['week'].max()
                else:
                    max_completed_week = 0
            except Exception as e:
                print(f"Error loading 2025 play-by-play data: {str(e)}")
                max_completed_week = 0
            
            # Find the next upcoming games from schedule
            if self.schedule_data is not None:
                try:
                    # Use Eastern Time for NFL scheduling consistency
                    from datetime import timezone, timedelta
                    est = timezone(timedelta(hours=-5))  # EST offset
                    today = datetime.now(est).date()
                    
                    # Look for games today or in the future
                    upcoming_games = self.schedule_data[
                        self.schedule_data['gameday'].dt.date >= today
                    ].sort_values('gameday')
                    
                    if not upcoming_games.empty:
                        next_week = upcoming_games['week'].iloc[0]
                        
                        # Additional logic: if it's Tuesday/Wednesday and we're between weeks,
                        # check if we should use the upcoming week
                        weekday = today.weekday()  # 0=Monday, 6=Sunday
                        
                        if weekday in [1, 2]:  # Tuesday or Wednesday
                            # Check if there are any remaining games in the max completed week
                            current_week_games = self.schedule_data[
                                (self.schedule_data['week'] == max_completed_week + 1) &
                                (self.schedule_data['gameday'].dt.date >= today)
                            ]
                            
                            if current_week_games.empty:
                                # No more games this week, move to next week
                                next_week = max_completed_week + 2
                        
                        print(f"Current week determined: {next_week} (max completed: {max_completed_week}, today: {today})")
                        return int(next_week)
                        
                except Exception as e:
                    print(f"Error determining week from schedule: {str(e)}")
            
            # Fallback to max completed week + 1
            fallback_week = int(max_completed_week) + 1
            print(f"Using fallback week: {fallback_week} (max completed: {max_completed_week})")
            return fallback_week
                
        except Exception as e:
            print(f"Could not determine current week: {str(e)}")
            return 3  # Conservative fallback
    
    def get_week_matchups(self, week_num=None):
        """Get actual matchups for a specific week from schedule data"""
        try:
            if self.schedule_data is None:
                if not self.load_schedule():
                    return []
            
            if week_num is None:
                week_num = self.get_current_week()
            
            week_games = self.schedule_data[self.schedule_data['week'] == week_num].copy()
            
            if week_games.empty:
                print(f"No games found for week {week_num}")
                return []
            
            matchups = []
            for _, game in week_games.iterrows():
                matchups.append({
                    'away_team': game['away_team'],
                    'home_team': game['home_team'],
                    'gameday': game['gameday'].strftime('%Y-%m-%d') if pd.notna(game['gameday']) else 'TBD',
                    'week': int(game['week'])
                })
            
            print(f"Found {len(matchups)} games for week {week_num}")
            return matchups
            
        except Exception as e:
            print(f"Error getting week {week_num} matchups: {str(e)}")
            return []
    
    def calculate_matchup_boosts(self, offense_team, defense_team):
        """Calculate TD boost for a specific matchup with percentage changes and detailed labels"""
        # FIXED: Only check current_2025, not baselines_2024
        if not self.data_loaded or not self.current_2025:
            try:
                self.load_data()
                self.data_loaded = True
            except Exception as e:
                print(f"Failed to load data in calculate_matchup_boosts: {str(e)}")
                return {"error": "Could not load required data"}
        
        results = {
            'matchup': f"{offense_team} vs {defense_team}",
            'offense_team': offense_team,
            'defense_team': defense_team,
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Red Zone Analysis - Percentage changes vs league averages
        rz_analysis = {}
        
        # Offense RZ performance vs league average (percentage change)
        if offense_team in self.current_2025['offense_rz']:
            current_off_rz = self.current_2025['offense_rz'][offense_team]['rz_td_rate']
            league_avg_rz_scoring = self.league_averages['rz_scoring']
            pct_change = ((current_off_rz - league_avg_rz_scoring) / league_avg_rz_scoring * 100) if league_avg_rz_scoring > 0 else 0
            rz_analysis['offense_rz_pct_change_vs_league'] = round(pct_change, 1)
            rz_analysis['offense_2025_rz_td_rate'] = current_off_rz
            rz_analysis['league_2024_rz_scoring_avg'] = league_avg_rz_scoring
        else:
            rz_analysis['offense_rz_pct_change_vs_league'] = None
            rz_analysis['note'] = f"Insufficient {offense_team} RZ data"
        
        # Defense RZ performance vs league average (percentage change)
        if defense_team in self.current_2025['defense_rz']:
            current_def_rz = self.current_2025['defense_rz'][defense_team]['rz_td_allow_rate']
            league_avg_rz_allow = self.league_averages['rz_allow']
            pct_change = ((current_def_rz - league_avg_rz_allow) / league_avg_rz_allow * 100) if league_avg_rz_allow > 0 else 0
            rz_analysis['defense_rz_pct_change_vs_league'] = round(pct_change, 1)
            rz_analysis['defense_2025_rz_allow_rate'] = current_def_rz
            rz_analysis['league_2024_rz_allow_avg'] = league_avg_rz_allow
        else:
            rz_analysis['defense_rz_pct_change_vs_league'] = None
        
        results['red_zone'] = rz_analysis
        
        # All Drives Analysis - Percentage changes vs league averages
        all_drives_analysis = {}
        
        # Offense all drives performance vs league average (percentage change)
        if offense_team in self.current_2025['offense_all']:
            current_off_all = self.current_2025['offense_all'][offense_team]['total_td_rate']
            league_avg_all_scoring = self.league_averages['all_drives_scoring']
            pct_change = ((current_off_all - league_avg_all_scoring) / league_avg_all_scoring * 100) if league_avg_all_scoring > 0 else 0
            all_drives_analysis['offense_all_drives_pct_change_vs_league'] = round(pct_change, 1)
            all_drives_analysis['offense_2025_all_drives_td_rate'] = current_off_all
            all_drives_analysis['league_2024_all_drives_scoring_avg'] = league_avg_all_scoring
        else:
            all_drives_analysis['offense_all_drives_pct_change_vs_league'] = None
        
        # Defense all drives performance vs league average (percentage change)
        if defense_team in self.current_2025['defense_all']:
            current_def_all = self.current_2025['defense_all'][defense_team]['total_td_allow_rate']
            league_avg_all_allow = self.league_averages['all_drives_allow']
            pct_change = ((current_def_all - league_avg_all_allow) / league_avg_all_allow * 100) if league_avg_all_allow > 0 else 0
            all_drives_analysis['defense_all_drives_pct_change_vs_league'] = round(pct_change, 1)
            all_drives_analysis['defense_2025_all_drives_allow_rate'] = current_def_all
            all_drives_analysis['league_2024_all_drives_allow_avg'] = league_avg_all_allow
        else:
            all_drives_analysis['defense_all_drives_pct_change_vs_league'] = None
        
        results['all_drives'] = all_drives_analysis
        
        # Combined Team Analysis - Average RZ and All Drives percentage changes
        combined_analysis = {}
        
        # Combined offense percentage change (average of RZ and all drives)
        off_rz_pct = rz_analysis.get('offense_rz_pct_change_vs_league')
        off_all_pct = all_drives_analysis.get('offense_all_drives_pct_change_vs_league')
        
        if off_rz_pct is not None and off_all_pct is not None:
            combined_analysis['offense_combined_pct_change'] = round((off_rz_pct + off_all_pct) / 2, 1)
        elif off_rz_pct is not None:
            combined_analysis['offense_combined_pct_change'] = off_rz_pct
        elif off_all_pct is not None:
            combined_analysis['offense_combined_pct_change'] = off_all_pct
        else:
            combined_analysis['offense_combined_pct_change'] = None
        
        # Combined defense percentage change (average of RZ and all drives)
        def_rz_pct = rz_analysis.get('defense_rz_pct_change_vs_league')
        def_all_pct = all_drives_analysis.get('defense_all_drives_pct_change_vs_league')
        
        if def_rz_pct is not None and def_all_pct is not None:
            combined_analysis['defense_combined_pct_change'] = round((def_rz_pct + def_all_pct) / 2, 1)
        elif def_rz_pct is not None:
            combined_analysis['defense_combined_pct_change'] = def_rz_pct
        elif def_all_pct is not None:
            combined_analysis['defense_combined_pct_change'] = def_all_pct
        else:
            combined_analysis['defense_combined_pct_change'] = None
        
        # Total team matchup advantage (average of offense and defense combined changes)
        off_combined = combined_analysis.get('offense_combined_pct_change')
        def_combined = combined_analysis.get('defense_combined_pct_change')
        
        if off_combined is not None and def_combined is not None:
            combined_analysis['total_team_td_advantage_pct'] = round((off_combined + def_combined) / 2, 1)
        elif off_combined is not None:
            combined_analysis['total_team_td_advantage_pct'] = round(off_combined / 2, 1)
        elif def_combined is not None:
            combined_analysis['total_team_td_advantage_pct'] = round(def_combined / 2, 1)
        else:
            combined_analysis['total_team_td_advantage_pct'] = None
        
        # Add explanations
        combined_analysis['explanation'] = {
            'offense_combined': f"Average of {offense_team} RZ and all-drives TD rate % change vs 2024 league averages",
            'defense_combined': f"Average of {defense_team} RZ and all-drives TD allow rate % change vs 2024 league averages", 
            'total_advantage': f"Overall team TD scoring advantage: average of offense boost and defense vulnerability",
            'calculation_note': "All red zone stats use 2+ plays filter for consistency with industry standards"
        }
        
        results['combined_team_analysis'] = combined_analysis
        
        return results
    
    def analyze_week_matchups(self, week_num=None):
        """Analyze all matchups for a specific week"""
        try:
            # FIXED: Only check current_2025 and data_loaded
            if not self.data_loaded or not self.current_2025:
                try:
                    self.load_data()
                    self.data_loaded = True
                except Exception as e:
                    return {"error": f"Could not load data: {str(e)}"}
            
            # Get current week matchups
            matchups = self.get_week_matchups(week_num)
            if not matchups:
                return {"error": "No matchups found", "week": week_num or self.get_current_week()}
            
            results = {
                'week': week_num or self.get_current_week(),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'games': []
            }
            
            print(f"Analyzing {len(matchups)} games for week {results['week']}...")
            
            for matchup in matchups:
                away_team = matchup['away_team']
                home_team = matchup['home_team']
                
                try:
                    # Analyze away team offense vs home team defense
                    away_offense_analysis = self.calculate_matchup_boosts(away_team, home_team)
                    
                    # Analyze home team offense vs away team defense  
                    home_offense_analysis = self.calculate_matchup_boosts(home_team, away_team)
                    
                    game_result = {
                        'game': f"{away_team} @ {home_team}",
                        'gameday': matchup['gameday'],
                        'week': matchup['week'],
                        'away_team': away_team,
                        'home_team': home_team,
                        'away_offense_vs_home_defense': away_offense_analysis,
                        'home_offense_vs_away_defense': home_offense_analysis
                    }
                    
                    results['games'].append(game_result)
                    
                except Exception as e:
                    print(f"Error analyzing {away_team} @ {home_team}: {str(e)}")
                    continue
            
            # Sort by highest total team advantages
            def get_sort_key(game):
                away_adv = game['away_offense_vs_home_defense'].get('combined_team_analysis', {}).get('total_team_td_advantage_pct', -999)
                home_adv = game['home_offense_vs_away_defense'].get('combined_team_analysis', {}).get('total_team_td_advantage_pct', -999)
                return max(away_adv or -999, home_adv or -999)
            
            results['games'].sort(key=get_sort_key, reverse=True)
            
            print(f"Week {results['week']} analysis complete!")
            return results
            
        except Exception as e:
            print(f"Error in analyze_week_matchups: {str(e)}")
            return {"error": f"Matchup analysis failed: {str(e)}"}

# Initialize the service
team_service = TeamAnalysisService()

@app.route('/')
def home():
    """Root endpoint"""
    return jsonify({
        "service": "NFL Team Analysis Service",
        "status": "running",
        "endpoints": [
            "/team-analysis - Get combined Vegas totals + TD boost analysis",
            "/health - Health check",
            "/refresh - Manual data refresh"
        ]
    })

@app.route('/team-analysis', methods=['GET'])
def get_team_analysis():
    """API endpoint to get combined Vegas totals + TD boost analysis"""
    try:
        results = team_service.get_team_analysis()
        return jsonify(results)
    except Exception as e:
        print(f"Error in team analysis endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/refresh', methods=['POST'])
def refresh_data_endpoint():
    """Manual data refresh endpoint"""
    try:
        team_service.refresh_data()
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
        "service": "Team Analysis Service",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "next_refresh": "Daily at 6:00 AM UTC"
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
