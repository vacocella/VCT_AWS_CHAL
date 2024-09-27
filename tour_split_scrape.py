# scrape.py

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, Boolean, ForeignKey,Numeric
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import time
from pymongo import MongoClient
from datetime import datetime
import pdb
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from datetime import date

# ----------------------- Configuration -----------------------

# Load environment variables from a .env file
load_dotenv()

# Base URL of the website
base_url = 'https://www.vlr.gg'

# PostgreSQL connection string
DATABASE_URL = os.getenv('DATABASE_URL')

# MongoDB connection string
MONGODB_URI = os.getenv('MONGODB_URI')

tour_url = 'https://www.vlr.gg/gc-2024'
tour_url = 'https://www.vlr.gg/vct-2024'

# ----------------------- Relational Database Setup (PostgreSQL) -----------------------

# Create engine and session for PostgreSQL
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


def seed_maps(session):
    valorant_maps = [
        Map(map_name="Ascent", active=True),
        Map(map_name="Bind", active=True),
        Map(map_name="Haven", active=True),
        Map(map_name="Split", active=True),
        Map(map_name="Icebox", active=True),
        Map(map_name="Breeze", active=True),
        Map(map_name="Fracture", active=True),
        Map(map_name="Pearl", active=True),
        Map(map_name="Lotus", active=True),
        Map(map_name="Sunset", active=True)
    ]
    
    # Adding all maps to the session
    session.add_all(valorant_maps)
    
    # Committing the session to save maps to the database
    session.commit()

seed_maps(session)

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
    real_name = Column(String(100))
    pp_url =  Column(String(300))
    region_id = Column(Integer, ForeignKey('regions.region_id'))
    
class Map(Base):
    __tablename__ = 'maps'
    map_id = Column(Integer, primary_key=True)
    map_name = Column(String(100))
    active = Column(Boolean, default=True)
    
class Tour(Base):
    __tablename__ = 'tours'
    tour_id = Column(Integer, primary_key=True)
    name = Column(String(1000))
    link = Column(String(1000))
    
class Tour_Split(Base):
    __tablename__ = 'tour_splits'
    external_split_id = Column(Integer, primary_key=True)
    tour_id = Column(Integer, ForeignKey('tours.tour_id'))
    name = Column(String(1000))
    link = Column(String(1000))
    start_date = Column(Date)  # Correct Date type
    end_date = Column(Date)    # Correct Date type
    prize_pool = Column(Numeric(precision=10, scale=2))  # Correct Numeric type for money
    location = Column(String(500))

class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True)
    team1_id = Column(Integer, ForeignKey('teams.team_id'))
    team2_id = Column(Integer, ForeignKey('teams.team_id'))
    map_id = Column(Integer, ForeignKey('maps.map_id'))
    date_played = Column(Date)
    document_id = Column(String(500))

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

def extract_event_details(event_header):
    event_desc_items = event_header.find_all('div', class_='event-desc-item')
    details = {}

    for item in event_desc_items:
        label_div = item.find('div', class_='event-desc-item-label')
        value_div = item.find('div', class_='event-desc-item-value')

        if label_div and value_div:
            label = label_div.text.strip()
            value = value_div.text.strip()

            if label == 'Dates':
                # Extract start_date and end_date
                start_date, end_date = parse_dates(value)
                details['start_date'] = start_date
                details['end_date'] = end_date
            elif label == 'Prize pool':
                prize_pool = parse_prize_pool(value)
                details['prize_pool'] = prize_pool
            elif label == 'Location':
                location = value
                details['location'] = location

    return details

def parse_dates(date_str):
    # Example date_str: 'Aug 1 - 25, 2024'
    try:
        if '-' in date_str:
            start_str, end_str = date_str.split('-')
            start_str = start_str.strip()
            end_str = end_str.strip()

            # Append year to start_str if missing
            if ',' not in start_str:
                # Assume the year is at the end of end_str
                year = end_str.split(',')[-1].strip()
                start_str += f", {year}"

            # Prepend month to end_str if missing
            if not any(month in end_str for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
                # Extract the month from start_str
                month = start_str.split(' ')[0]
                end_str = f"{month} {end_str}"

            # Now parse the dates
            start_date = datetime.strptime(start_str, '%b %d, %Y').date()
            end_date = datetime.strptime(end_str, '%b %d, %Y').date()
        else:
            # Single date event
            start_date = datetime.strptime(date_str, '%b %d, %Y').date()
            end_date = start_date

        return start_date, end_date

    except Exception as e:
        print(f"Error parsing dates: {e}")
        return None, None

def parse_prize_pool(prize_str):
    # Example prize_str: '$2,250,000 USD'
    try:
        # Remove currency symbols and commas
        amount_str = prize_str.replace('$', '').replace(',', '').split()[0]
        prize_pool = float(amount_str)
        return prize_pool
    except Exception as e:
        print(f"Error parsing prize pool: {e}")
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

def get_tour(tour_name, tour_link):
    try:
        # Correctly compare the 'name' column to 'tour_name'
        print(tour_link,tour_name)
        
        tour = session.query(Tour).filter(Tour.name == tour_name).first()
        print("tour",tour)
                
        if not tour:
            tour = Tour(name=tour_name, link=tour_link)
            session.add(tour)
            session.commit()
            print(f"Inserted tour '{tour_name}' into PostgreSQL.")
        else:
            print(f"Tour '{tour_name}' already exists in PostgreSQL.")
        return tour.tour_id
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error while getting/creating tour '{tour_name}': {e}")
        return None

def get_tour_split(external_split_id, tour_id, name, link, start_date, end_date, prize_pool, location):
    try:
        # Correct query using the column, not the class
        print(external_split_id,tour_id,name,link,start_date,end_date,prize_pool,location)
        
        tour_split = session.query(Tour_Split).filter(Tour_Split.external_split_id == external_split_id).first()
        print(tour_split)
        if not tour_split:
            tour_split = Tour_Split(
                external_split_id=external_split_id,
                tour_id=tour_id,
                name=name,
                link=link,
                start_date=start_date,
                end_date=end_date,
                prize_pool=prize_pool,
                location=location
            )
            session.add(tour_split)
            session.commit()
            print(f"Inserted tour split '{name}' into PostgreSQL.")
        else:
            print(f"Tour split '{name}' already exists in PostgreSQL.")
        return tour_split.external_split_id
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error while getting/creating tour split '{name}': {e}")
        input()
        return None

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

    game_response = requests.get(full_game_url)
    game_soup = BeautifulSoup(game_response.content, 'html.parser')

    # Extract match details
    match_header_super = game_soup.find('div', class_='match-header-super')
    event_link = match_header_super.find('a', class_='match-header-event')['href']
    
    date_div = match_header_super.find('div', {'data-utc-ts': True})
    # pdb.set_trace()
    date_played = date_div['data-utc-ts'] if date_div else None
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
    if existing_match:
        return 

    # Prepare game data for MongoDB
    game_data = {
        "game_id": f"game_{match_id}",
        "map": "Unknown", 
        "teams": [],
        "rounds": [],  
        "event": {
            "event_name": event_name,
            "date": date_played if date_played else None,
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
    result = games_collection.insert_one(game_data)

    # Retrieve and store the inserted document's ID
    document_id = result.inserted_id
    
    new_match = Match(
            match_id=match_id,
            team1_id=team1_id,
            team2_id=team2_id,
            map_id=map_id,
            date_played=date_played,
            document_id=document_id
        )
    session.add(new_match)
    session.commit()
    print(f"Inserted game data for match {match_id} into MongoDB.")

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
    player_img_url = player_header.find('img')['src']
    player_name_div = player_header.find('h1', class_='wf-title') if player_header else None
    player_name_real_div = player_header.find('h2', class_='player-real-name') if player_header else None
    player_name = player_name_div.text.strip() if player_name_div else None
    player_name_real = player_name_real_div.text.strip() if player_name_real_div else None

    # # Get team and region information (adjust as needed based on actual HTML structure)
    # team_link = player_header.find('a', class_='player-header-team-name')
    # team_id = get_team(team_link['href']) if team_link else None

    region_name = player_header.find('div', class_='ge-text-light').text.strip()
    region_id = get_region(region_name)

    # Insert player into PostgreSQL
    new_player = Player(
        player_id=player_id,
        name=player_name,
        real_name=player_name_real,
        pp_url= player_img_url,
        # team_id=team_id,
        region_id=region_id
    )
    session.add(new_player)
    session.commit()
    print(f"Inserted player {player_name} (ID: {player_id}) into PostgreSQL.")

def scrape_split(split_url, tour_id):
    response = requests.get(split_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    nav_bar = soup.find('div', class_='wf-nav')
    
    nav_items = nav_bar.find_all('a', class_='wf-nav-item')
    
     # Extract the external_split_id from the URL
    parsed_url = urlparse(split_url)
    path_parts = parsed_url.path.strip('/').split('/')
    external_split_id = int(path_parts[1]) if len(path_parts) > 1 else None
    print(external_split_id)
    
     # Find the event header
    event_header = soup.find('div', class_='event-header')
    if event_header is None:
        print("Error: 'event-header' div not found.")
        return

    # Extract split details
    split_name_div = event_header.find('h1', class_='wf-title')
    if split_name_div:
        split_name = split_name_div.text.strip()
    else:
        split_name = 'Unknown Split'

    # Extract additional details
    details = extract_event_details(event_header)
    start_date = details.get('start_date')
    end_date = details.get('end_date')
    prize_pool = details.get('prize_pool')
    location = details.get('location')

    # Extract external_split_id from the URL
    parsed_url = urlparse(split_url)
    path_parts = parsed_url.path.strip('/').split('/')
    external_split_id = int(path_parts[1]) if len(path_parts) > 1 else None

    # Get or create the tour split
    get_tour_split(
        external_split_id=external_split_id,
        tour_id=tour_id,
        name=split_name,
        link=split_url,
        start_date=start_date,
        end_date=end_date,
        prize_pool=prize_pool,
        location=location
    )
    
    matches_in_split = nav_items[1]['href']
    # Extract series_id from the URL
    
    team_container = soup.find('div', class_='event-teams-container')
    teams = team_container.find_all('div', class_='wf-card event-team')
    
    for team in teams:
        team_link_tag = team.find('a', class_='event-team-name')
        if not team_link_tag:
            print("Team link not found.")
            continue
        
        team_link = team_link_tag.get('href', '')
        team_name = team_link_tag.text.strip()
        print(f"Team Name: {team_name}, Team Link: {team_link}")
        team_id = get_team(team_link)
    
    
    # scrape matches
    response2 = requests.get(base_url + matches_in_split)
    print(base_url + matches_in_split)
    soup_matches = BeautifulSoup(response2.content, 'html.parser')
    matches = soup_matches.find('a', class_='wf-module-item match-item mod-color')
    
    for match in matches:
        match_link = match['href'] 


def scrape_tour_data():
    response = requests.get(tour_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find player links
    event_header = soup.find('div', class_='event-header')
    tour_title = event_header.find('div', class_='wf-title').text
    tour_link = tour_url 
    tour_id = get_tour(tour_title.strip(), tour_link)
        
    events = soup.find_all('a', class_='wf-card mod-flex event-item')
    print(tour_id)
    for row in events:
        try:
            print(row['href'])
            split_link = base_url + row['href']
            scrape_split(split_link, tour_id)
        except Exception as e:
            print(e)

# ----------------------- Main Execution -----------------------

if __name__ == "__main__":
    try:
        scrape_tour_data()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the session when done
        session.close()
        # Close MongoDB connection
        mongo_client.close()

# Idea: Go through tours 
# get all splits 
# 