# scrape.py

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import time
from pymongo import MongoClient
from datetime import datetime

# ----------------------- Configuration -----------------------

# Load environment variables from a .env file
load_dotenv()

# Base URL of the website
base_url = 'https://www.vlr.gg'

# PostgreSQL connection string
DATABASE_URL = os.getenv('DATABASE_URL')

# MongoDB connection string
MONGODB_URI = os.getenv('MONGODB_URI')

# ----------------------- Relational Database Setup (PostgreSQL) -----------------------

# Create engine and session for PostgreSQL
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Define models according to your schema
class Region(Base):
    __tablename__ = 'regions'
    region_id = Column(Integer, primary_key=True)
    region_name = Column(String(100))
    larger_region_id = Column(Integer, ForeignKey('regions.region_id'), nullable=True)

    larger_region = relationship('Region', remote_side=[region_id])

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String(100))
    region_id = Column(Integer, ForeignKey('regions.region_id'))
    founding_date = Column(Date, nullable=True)
    active = Column(Boolean, default=True)

    region = relationship('Region')

class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    role = Column(String(50))
    region_id = Column(Integer, ForeignKey('regions.region_id'))

    team = relationship('Team')
    region = relationship('Region')

class Map(Base):
    __tablename__ = 'maps'
    map_id = Column(Integer, primary_key=True)
    map_name = Column(String(100))
    active = Column(Boolean, default=True)

class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True)
    team1_id = Column(Integer, ForeignKey('teams.team_id'))
    team2_id = Column(Integer, ForeignKey('teams.team_id'))
    map_id = Column(Integer, ForeignKey('maps.map_id'))
    date_played = Column(Date)
    team1_score = Column(Integer)
    team2_score = Column(Integer)

    team1 = relationship('Team', foreign_keys=[team1_id])
    team2 = relationship('Team', foreign_keys=[team2_id])
    map = relationship('Map')

class MatchHistoryPlayer(Base):
    __tablename__ = 'match_history_player'
    match_id = Column(Integer, ForeignKey('matches.match_id'), primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    agent = Column(String(50))
    kills = Column(Integer)
    assists = Column(Integer)
    deaths = Column(Integer)
    acs = Column(Float)
    kast = Column(Float)
    adr = Column(Float)
    first_kills = Column(Integer)
    first_deaths = Column(Integer)

    match = relationship('Match')
    player = relationship('Player')
    team = relationship('Team')

# Create tables in the database
Base.metadata.create_all(engine)

# ----------------------- NoSQL Database Setup (MongoDB) -----------------------

# Create MongoDB client and access database and collections
mongo_client = MongoClient(MONGODB_URI)
mongo_db = mongo_client['valorantdb']  # You can name the database as you prefer
games_collection = mongo_db['games']

# ----------------------- Helper Functions -----------------------

def extract_player_id_from_url(player_url):
    url_parts = player_url.strip('/').split('/')
    try:
        idx = url_parts.index('player')
        player_id = int(url_parts[idx + 1])
        return player_id
    except (ValueError, IndexError):
        return None  # Unable to extract player_id

def parse_stat(stat_text):
    stat_values = stat_text.strip().replace('%', '').split('\n')
    float_values = []
    for val in stat_values:
        val = val.strip()
        if val == '/' or not val:
            continue
        try:
            float_values.append(float(val))
        except ValueError:
            continue
    if len(float_values) == 1:
        return float_values[0]
    elif len(float_values) > 1:
        return sum(float_values) / len(float_values)
    else:
        return None

# ----------------------- Scraping Functions -----------------------

def get_region(region_name):
    # Check if region exists
    region = session.query(Region).filter_by(region_name=region_name).first()
    if not region:
        # Create new region
        region = Region(region_name=region_name)
        session.add(region)
        session.commit()
    return region.region_id

def get_team(team_link):
    # Extract the team ID from the link
    team_id = int(team_link.split('/')[2])
    team_url = base_url + team_link
    team_res = requests.get(team_url)
    team_soup = BeautifulSoup(team_res.content, 'html.parser')

    team_header = team_soup.find('div', class_='team-header')
    team_name = team_header.find('h1', class_='wf-title').text.strip()

    # Get region (you may need to adjust this based on actual data available)
    region_name = team_header.find('div', class_='team-header-country').text.strip()
    region_id = get_region(region_name)

    # Check if team already exists
    existing_team = session.query(Team).filter_by(team_id=team_id).first()
    if not existing_team:
        # Insert team into database
        new_team = Team(team_id=team_id, team_name=team_name, region_id=region_id)
        session.add(new_team)
        session.commit()
        print(f"Inserted team {team_name} (ID: {team_id}) into PostgreSQL.")
    else:
        print(f"Team {team_name} (ID: {team_id}) already exists in PostgreSQL.")
    return team_id

def scrape_game_data(game_url):
    # Extract match_id from URL
    match_id = int(game_url.split('/')[1])
    
    # Check if the match already exists in MongoDB
    existing_game = games_collection.find_one({'game_id': f'game_{match_id}'})
    if existing_game:
        print(f"Game with game_id game_{match_id} already exists in MongoDB.")
        return

    full_game_url = base_url + game_url
    print(f"Scraping game: {full_game_url}")
    input()

    game_response = requests.get(full_game_url)
    game_soup = BeautifulSoup(game_response.content, 'html.parser')

    # Extract match details
    match_header_super = game_soup.find('div', class_='match-header-super')
    date_div = match_header_super.find('div', {'data-utc-ts': True})
    date_played = datetime.fromtimestamp(int(date_div['data-utc-ts']) / 1000).date() if date_div else None
    tournament_div = match_header_super.find('div', style='font-weight: 700;')
    event_name = tournament_div.text.strip() if tournament_div else None
    patch_div = match_header_super.find('div', style='font-style: italic;')
    patch = patch_div.text.strip() if patch_div else None

    # Extract team information
    match_header_vs = game_soup.find('div', class_='match-header-vs')
    team1_div = match_header_vs.find('div', class_='match-header-link-name mod-1')
    team2_div = match_header_vs.find('div', class_='match-header-link-name mod-2')
    team1_name = team1_div.find('div', class_='wf-title-med').text.strip()
    team2_name = team2_div.find('div', class_='wf-title-med').text.strip()
    team1_link = team1_div.find_parent('a')['href']
    team2_link = team2_div.find_parent('a')['href']
    team1_id = get_team(team1_link)
    team2_id = get_team(team2_link)

    # Extract scores
    scores_div = match_header_vs.find('div', class_='match-header-vs-score')
    scores = scores_div.find_all('span') if scores_div else []
    team1_score = int(scores[0].text.strip()) if scores else None
    team2_score = int(scores[-1].text.strip()) if scores else None

    # For simplicity, we'll use a placeholder map_id
    map_id = 1  # You can adjust this to match actual map data

    # Insert match data into PostgreSQL
    existing_match = session.query(Match).filter_by(match_id=match_id).first()
    if not existing_match:
        new_match = Match(
            match_id=match_id,
            team1_id=team1_id,
            team2_id=team2_id,
            map_id=map_id,
            date_played=date_played,
            team1_score=team1_score,
            team2_score=team2_score
        )
        session.add(new_match)
        session.commit()
        print(f"Inserted match {match_id} into PostgreSQL.")
    else:
        print(f"Match {match_id} already exists in PostgreSQL.")

    # Prepare game data for MongoDB
    game_data = {
        "game_id": f"game_{match_id}",
        "map": "Unknown",  # You can extract actual map data if available
        "teams": [],
        "rounds": [],  # Add round data if available
        "event": {
            "event_name": event_name,
            "date": date_played.strftime('%Y-%m-%d') if date_played else None,
            "patch": patch
        }
    }

    # Process each team's player statistics
    vm_stats_games = game_soup.find_all('div', class_='vm-stats-game')

    for game_div in vm_stats_games:
        # Extract map name
        map_name_div = game_div.find('div', class_='map')
        map_name_span = map_name_div.find('span') if map_name_div else None
        map_name = map_name_span.text.strip() if map_name_span else None
        if map_name:
            game_data['map'] = map_name

        player_tables = game_div.find_all('table', class_='wf-table-inset mod-overview')
        team_ids = [team1_id, team2_id]
        team_names = [team1_name, team2_name]

        for idx, table in enumerate(player_tables):
            team_id = team_ids[idx]
            team_name = team_names[idx]
            team_data = {
                "team_id": team_id,
                "team_name": team_name,
                "players": []
            }
            tbody = table.find('tbody')
            rows = tbody.find_all('tr') if tbody else []
            for row in rows:
                player_td = row.find('td', class_='mod-player')
                player_name_div = player_td.find('div', class_='text-of') if player_td else None
                player_name = player_name_div.text.strip() if player_name_div else None
                player_href = player_td.find('a')['href'] if player_td else None
                player_id = extract_player_id_from_url(player_href)

                if player_id:
                    # Check if player exists in the database
                    existing_player = session.query(Player).filter_by(player_id=player_id).first()
                    if not existing_player:
                        # Scrape player details
                        scrape_player_page(player_href)
                    # Extract player stats
                    stats_tds = row.find_all('td')

                    # Agent
                    agent = stats_tds[1].find('img')['title'] if stats_tds[1].find('img') else None

                    # Statistics
                    kills = parse_stat(stats_tds[4].text)
                    deaths = parse_stat(stats_tds[5].text)
                    assists = parse_stat(stats_tds[6].text)
                    acs = parse_stat(stats_tds[3].text)
                    kast = parse_stat(stats_tds[8].text)
                    adr = parse_stat(stats_tds[9].text)
                    first_kills = parse_stat(stats_tds[11].text)
                    first_deaths = parse_stat(stats_tds[12].text)

                    # Insert into MatchHistoryPlayer
                    new_match_history = MatchHistoryPlayer(
                        match_id=match_id,
                        player_id=player_id,
                        team_id=team_id,
                        agent=agent,
                        kills=kills,
                        assists=assists,
                        deaths=deaths,
                        acs=acs,
                        kast=kast,
                        adr=adr,
                        first_kills=first_kills,
                        first_deaths=first_deaths
                    )
                    session.merge(new_match_history)
                    session.commit()
                    print(f"Inserted stats for player {player_id} in match {match_id}.")

                    # Prepare player data for MongoDB
                    player_data = {
                        "player_id": player_id,
                        "agent": agent,
                        "kills": kills,
                        "deaths": deaths,
                        "assists": assists,
                        "acs": acs,
                        "kast": f"{kast}%",
                        "adr": adr,
                        "first_kills": first_kills,
                        "first_deaths": first_deaths
                    }
                    team_data["players"].append(player_data)

            game_data["teams"].append(team_data)

        # Since we process only one game_div, break after first iteration
        break

    # Insert game data into MongoDB
    games_collection.insert_one(game_data)
    print(f"Inserted game data for match {match_id} into MongoDB.")

    # Delay to be respectful to the website's server
    time.sleep(1)

def scrape_player_page(player_url):
    player_id = extract_player_id_from_url(player_url)
    if not player_id:
        print(f"Could not extract player_id from {player_url}")
        return

    # Check if player already exists in the database
    existing_player = session.query(Player).filter_by(player_id=player_id).first()
    if existing_player:
        print(f"Player {existing_player.name} (ID: {player_id}) already exists in PostgreSQL.")
        return

    internal_response = requests.get(base_url + player_url)
    internal_soup = BeautifulSoup(internal_response.content, 'html.parser')

    # Extract player details
    player_header = internal_soup.find('div', class_='player-header')
    player_name_div = player_header.find('h1', class_='wf-title') if player_header else None
    player_name = player_name_div.text.strip() if player_name_div else None

    # Get team and region information (adjust as needed based on actual HTML structure)
    team_link = player_header.find('a', class_='player-header-team-name')
    team_id = get_team(team_link['href']) if team_link else None

    region_name = player_header.find('div', class_='ge-text-light').text.strip()
    region_id = get_region(region_name)

    # Insert player into PostgreSQL
    new_player = Player(
        player_id=player_id,
        name=player_name,
        team_id=team_id,
        role=None,  # Adjust if you can extract the role
        region_id=region_id
    )
    session.add(new_player)
    session.commit()
    print(f"Inserted player {player_name} (ID: {player_id}) into PostgreSQL.")

def scrape_data():
    response = requests.get(base_url + '/stats')
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find player links
    table = soup.find('div', class_='wf-card mod-table mod-dark')
    tbody = table.find('tbody') if table else None
    rows = tbody.find_all('tr') if tbody else []

    for row in rows:
        player_td = row.find('td', class_='mod-player mod-a')
        if player_td:
            player_url = player_td.find('a')['href']
            player_matches_url = player_url.replace('/player/', '/player/matches/')
            scrape_player_page(player_url)
            time.sleep(1)  # Delay between requests

            # Scrape matches the player has participated in
            internal_response = requests.get(base_url + player_matches_url)
            internal_soup = BeautifulSoup(internal_response.content, 'html.parser')
            match_links = internal_soup.find_all('a', href=True, class_='wf-card fc-flex m-item')
            print(base_url + player_matches_url)
            for a_tag in match_links:
                scrape_game_data(a_tag['href'])
                time.sleep(1)  # Delay between requests

# ----------------------- Main Execution -----------------------

if __name__ == "__main__":
    try:
        scrape_data()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the session when done
        session.close()
        # Close MongoDB connection
        mongo_client.close()
