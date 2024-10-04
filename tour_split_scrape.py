# scrape.py

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine,PrimaryKeyConstraint, Column, String, Integer, Float, Date, Boolean, ForeignKey,Numeric
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import pdb
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from datetime import date
import re

# ----------------------- Configuration -----------------------

# Load environment variables from a .env file
load_dotenv()

# Base URL of the website
base_url = 'https://www.vlr.gg'

# PostgreSQL connection string
DATABASE_URL = os.getenv('DATABASE_URL')

# MongoDB connection string
# MONGODB_URI = os.getenv('MONGODB_URI')

all_tours = ['https://www.vlr.gg/vct-2024','https://www.vlr.gg/gc-2024',
'https://www.vlr.gg/vcl-2024',
]

# ----------------------- Relational Database Setup (PostgreSQL) -----------------------

# Create engine and session for PostgreSQL
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Define models according to your schema

class Parent_Region(Base):
    __tablename__ = 'parent_regions'
    parent_region_id = Column(Integer, primary_key=True)
    parent_region_name = Column(String(100))

class Region(Base):
    __tablename__ = 'regions'
    region_id = Column(Integer, primary_key=True)
    region_name = Column(String(100))

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String(100))
    region_id = Column(Integer, ForeignKey('regions.region_id'))
    team_img_url =  Column(String(100))
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
    
class Agent(Base):
    __tablename__ = 'agents'
    agent_id = Column(Integer, primary_key=True)
    agent_name = Column(String(50))
    
class Tour(Base):
    __tablename__ = 'tours'
    tour_id = Column(Integer, primary_key=True)
    name = Column(String(1000))
    link = Column(String(1000))
    
class Tour_Split(Base):
    __tablename__ = 'tour_splits'
    external_split_id = Column(Integer, primary_key=True)
    tour_id = Column(Integer, ForeignKey('tours.tour_id'))
    parent_region_id = Column(Integer, ForeignKey('parent_regions.parent_region_id'))
    name = Column(String(1000))
    link = Column(String(1000))
    start_date = Column(Date)  # Correct Date type
    end_date = Column(Date)    # Correct Date type
    prize_pool = Column(Numeric(precision=10, scale=2))  # Correct Numeric type for money
    location = Column(String(500))

class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True)
    tour_split_id = Column(Integer, ForeignKey('tour_splits.external_split_id'))
    team1_id = Column(Integer, ForeignKey('teams.team_id'))
    team2_id = Column(Integer, ForeignKey('teams.team_id'))
    date_played = Column(Date)
    
class Game(Base):
    __tablename__ = 'games'
    game_id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'))
    map_id = Column(Integer, ForeignKey('maps.map_id'))

class PlayerRole(Base):
    __tablename__ = 'player_roles'
    role_id = Column(Integer, primary_key=True)
    role_name = Column(String(100))
    
class GamePlayer(Base):
    __tablename__ = 'game_players'
    game_id = Column(Integer, ForeignKey('games.game_id'))
    player_id = Column(Integer, ForeignKey('players.player_id'))
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    agent = Column(Integer, ForeignKey('agents.agent_id'))
    player_role = Column(Integer, ForeignKey('player_roles.role_id'))
    ct_and_t_data = Column(Boolean)
    
    # CT-side statistics
    ct_kills = Column(Integer)
    ct_assists = Column(Integer)
    ct_deaths = Column(Integer)
    ct_acs = Column(Float)
    ct_kast = Column(Float)
    ct_adr = Column(Float)
    ct_hs = Column(Float)
    ct_first_kills = Column(Integer)
    ct_first_deaths = Column(Integer)

    # T-side statistics
    t_kills = Column(Integer)
    t_assists = Column(Integer)
    t_deaths = Column(Integer)
    t_acs = Column(Float)
    t_kast = Column(Float)
    t_adr = Column(Float)
    t_hs = Column(Float)
    t_first_kills = Column(Integer)
    t_first_deaths = Column(Integer)
    
    # both statistics
    both_kills = Column(Integer)
    both_assists = Column(Integer)
    both_deaths = Column(Integer)
    both_acs = Column(Float)
    both_kast = Column(Float)
    both_adr = Column(Float)
    both_hs = Column(Float)
    both_first_kills = Column(Integer)
    both_first_deaths = Column(Integer)
    
    __table_args__ = (
        PrimaryKeyConstraint('game_id', 'player_id'),
    )

# Create tables in the database
Base.metadata.create_all(engine)

# Populate Maps
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
        Map(map_name="Sunset", active=True),
        Map(map_name="Unknown", active=True)
    ]
    
    # Adding all maps to the session
    session.add_all(valorant_maps)
    
    # Committing the session to save maps to the database
    session.commit()

seed_maps(session)

# Function to seed agents into the database
def seed_agents(session):
    valorant_agents = [
        Agent(agent_name="Brimstone"),
        Agent(agent_name="Viper"),
        Agent(agent_name="Omen"),
        Agent(agent_name="Killjoy"),
        Agent(agent_name="Cypher"),
        Agent(agent_name="Sova"),
        Agent(agent_name="Sage"),
        Agent(agent_name="Phoenix"),
        Agent(agent_name="Jett"),
        Agent(agent_name="Reyna"),
        Agent(agent_name="Raze"),
        Agent(agent_name="Breach"),
        Agent(agent_name="Skye"),
        Agent(agent_name="Yoru"),
        Agent(agent_name="Astra"),
        Agent(agent_name="KAY/O"),
        Agent(agent_name="Chamber"),
        Agent(agent_name="Neon"),
        Agent(agent_name="Fade"),
        Agent(agent_name="Harbor"),
        Agent(agent_name="Gekko"),
        Agent(agent_name="Deadlock"),
        Agent(agent_name="Unknown")
    ]
    
    # Adding all agents to the session
    session.add_all(valorant_agents)
    
    # Committing the session to save agents to the database
    session.commit()

# Seeding agents into the database
seed_agents(session)

def seed_regions(session):
    valorant_regions = [
        Parent_Region(parent_region_name="America"),
        Parent_Region(parent_region_name="EMEA"),
        Parent_Region(parent_region_name="Pacific"),
        Parent_Region(parent_region_name="China"),
    ]
    
    # Adding all valorant_regions to the session
    session.add_all(valorant_regions)
    
    # Committing the session to save valorant_regions to the database
    session.commit()

# Seeding valorant_regions into the database
seed_regions(session)

valorant_maps = [
    "Ascent",  
    "Bind",    
    "Haven",   
    "Split",   
    "Icebox",  
    "Breeze",  
    "Fracture",
    "Pearl",  
    "Lotus",   
    "Sunset"   
]

agent_names = [
    "Brimstone",
    "Viper",
    "Omen",
    "Killjoy",
    "Cypher",
    "Sova",
    "Sage",
    "Phoenix",
    "Jett",
    "Reyna",
    "Raze",
    "Breach",
    "Skye",
    "Yoru",
    "Astra",
    "KAY/O",
    "Chamber",
    "Neon",
    "Fade",
    "Harbor",
    "Gekko",
    "Deadlock",
    "Unknown"
]

parent_regions = [
    'americas',
    'emea',
    'pacific',
    'china',
]

# ----------------------- NoSQL Database Setup (MongoDB) -----------------------

# Create MongoDB client and access database and collections
# mongo_client = MongoClient(MONGODB_URI)
# mongo_db = mongo_client['valorantdb']  # You can name the database as you prefer
# games_collection = mongo_db['games']

# ----------------------- Helper Functions -----------------------


def valid_img_url(img_url):
    if "owcdn" in img_url:
        # Use regex to extract the ID before the file extension
        match = re.search(r'([^/]+)(?=\.\w+$)', img_url)
        if match:
            return match.group(0)
        else:
                return ""
    else:
        return ""    

def extract_player_id_from_url(player_url):

    url_parts = player_url.strip('/').split('/')
    try:
        idx = url_parts.index('player')
        player_id = int(url_parts[idx + 1])
        return player_id
    except (ValueError, IndexError):
        return None  # Unable to extract player_id

def parse_stat(stat_text):
    stat_text = stat_text.replace('\xa0', '').replace('&nbsp;', '').strip()
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
        return 0

def parse_sides_stat(stat_td):
    
    t = stat_td.find('span', class_="mod-t") 
    ct = stat_td.find('span', class_="mod-ct")
    both = stat_td.find('span', class_="mod-both")
    
    t_side = 0
    ct_side = 0
    both_side = 0
    side_data = False
    
    if t and ct:
        t_side = parse_stat(t.text)
        ct_side = parse_stat(ct.text)
        side_data = True
        
        
    if both:
        both_side = parse_stat(both.text)
    
    return {"t": t_side, "ct": ct_side, "both": both_side, "side_data": side_data }

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

def insert_or_get_game_player(game_id, player_id, team_id, agent, player_role,side_data, ct_kills=0, ct_assists=0, ct_deaths=0, 
                              ct_acs=0.0, ct_kast=0.0, ct_adr=0.0, ct_first_kills=0, ct_first_deaths=0,
                              t_kills=0, t_assists=0, t_deaths=0, t_acs=0.0, t_kast=0.0, t_adr=0.0, 
                              t_first_kills=0, t_first_deaths=0, both_kills=0, both_assists=0, both_deaths=0, both_acs=0.0, both_kast=0.0, both_adr=0.0, 
                              both_first_kills=0, both_first_deaths=0, t_hs=0, ct_hs=0, both_hs=0):
    # Check if the GamePlayer already exists
    existing_game_player = session.query(GamePlayer).filter_by(game_id=game_id, player_id=player_id).first()
    
    if not existing_game_player:
        # Insert the GamePlayer into the database
        try:
        
            new_game_player = GamePlayer(
                game_id=game_id,
                player_id=player_id,
                team_id=team_id,
                agent=agent,
                player_role=player_role,
                ct_and_t_data=side_data,
                ct_kills=ct_kills,
                ct_assists=ct_assists,
                ct_deaths=ct_deaths,
                ct_acs=ct_acs,
                ct_kast=ct_kast,
                ct_adr=ct_adr,
                ct_first_kills=ct_first_kills,
                ct_first_deaths=ct_first_deaths,
                t_kills=t_kills,
                t_assists=t_assists,
                t_deaths=t_deaths,
                t_acs=t_acs,
                t_kast=t_kast,
                t_adr=t_adr,
                t_first_kills=t_first_kills,
                t_first_deaths=t_first_deaths,
                both_kills=both_kills,
                both_assists=both_assists,
                both_deaths=both_deaths,
                both_acs=both_acs,
                both_kast=both_kast,
                both_adr=both_adr,
                both_first_kills=both_first_kills,
                both_first_deaths=both_first_deaths,
                ct_hs=ct_hs,
                t_hs=t_hs,
                both_hs=both_hs
            )
            session.add(new_game_player)
            session.commit()
            print(f"Inserted new GamePlayer record for player_id {player_id} and game_id {game_id}.")
        except Exception as e:
            print(e)
            
            print(game_id, player_id, team_id, agent, player_role, ct_kills, ct_assists, ct_deaths,
                  ct_acs, ct_kast, ct_adr, ct_first_kills, ct_first_deaths)
            print('xxxxxxxxxxxxxxxxx')
            print(t_kills, t_assists, t_deaths, t_acs, t_kast, t_adr, 
                              t_first_kills, t_first_deaths)
            
    else:
        print(f"GamePlayer record for player_id {player_id} and game_id {game_id} already exists.")
        input()
    
    return existing_game_player or new_game_player

def insert_or_get_game(game_id, match_id, map_id):
    # Check if the Game already exists
    existing_game = session.query(Game).filter_by(game_id=game_id).first()
    
    if not existing_game:
        # Insert the Game into the database
        new_game = Game(
            game_id=game_id,
            match_id=match_id,
            map_id=map_id
        )
        session.add(new_game)
        session.commit()
        print(f"Inserted new Game record with game_id {game_id} and match_id {match_id}.")
    else:
        print(f"Game record with game_id {game_id} already exists.")
    
    # Return the existing or new Game object
    return existing_game or new_game
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
        input()
        return None

def get_team(team_link):
    # Extract the team ID from the link
    
    team_id = int(team_link.split('/')[2])
    team_url = base_url + team_link
    team_res = requests.get(team_url)
    team_soup = BeautifulSoup(team_res.content, 'html.parser')

    team_header = team_soup.find('div', class_='team-header')
    avatar_div = team_soup.find('div', class_='wf-avatar')
    team_img = avatar_div.find('img')['src']
    team_name = team_header.find('h1', class_='wf-title').text.strip()

    # Get region (you may need to adjust this based on actual data available)
    region_name = team_header.find('div', class_='team-header-country').text.strip()
    region_id = get_region(region_name)

    # Check if team already exists
    existing_team = session.query(Team).filter_by(team_id=team_id).first()
    if not existing_team:
        # Insert team into database
        new_team = Team(team_id=team_id, team_name=team_name, region_id=region_id, team_img_url= valid_img_url(team_img))
        session.add(new_team)
        session.commit()
        print(f"Inserted team {team_name} (ID: {team_id}) into PostgreSQL.")
    else:
        print(f"Team {team_name} (ID: {team_id}) already exists in PostgreSQL.")
    return team_id

def scrape_game_data(game_url,tour_split_id):
    # Extract match_id from URL
    match_id = int(game_url.split('/')[1])

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

    # Insert match data into PostgreSQL
    existing_match = session.query(Match).filter_by(match_id=match_id).first()
    if existing_match:
        return 

    # Process each team's player statistics
    vm_stats_games = game_soup.find_all('div', class_='vm-stats-game')
    
    match = {
        "match_id": match_id,
        "teams": [team1_id,team2_id],
        "event": {
                "event_name": event_name,
                "date": date_played if date_played else None,
                "patch": patch
            },
        "games":[]
    }
    
    new_match = Match(
            match_id=match_id,
            team1_id=team1_id,
            team2_id=team2_id,
            tour_split_id=tour_split_id,

            date_played=date_played
        )
    
    session.add(new_match)
    session.commit()
    print(f"Inserted game data for match {match_id} into post.")

    for game_div in vm_stats_games:
        
        game_id = game_div.get('data-game-id')
        if game_id == 'all': continue
        
        game_data = {
            "game_id": game_id,
            "map": "Unknown",
            "teams": []
        }
        
        # Extract map name
        map_name_div = game_div.find('div', class_='map')
        map_name_span = map_name_div.find('span') if map_name_div else None
        map_name = map_name_span.text.strip() if map_name_span else None
        if map_name:
            game_data['map'] = ''.join(str(map_name).split()).replace("PICK", "")
        
        map_id = 11
        if game_data['map'] in valorant_maps:
            map_id = valorant_maps.index(game_data['map']) + 1
            
        insert_or_get_game(game_id, match_id, map_id)
        
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
                agents_td = row.find('td', class_='mod-agents')
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

                    # Agents
                    agents = []
                    
                    agents_spans = agents_td.find_all('span')
                    
                    for agent in agents_spans:
                        agents.append(agent.find('img')['title'])
                    
                    agent_id = 23
                    if agents[0] in agent_names:
                        agent_id = agent_names.index(agents[0]) + 1
                    
                    # Statistics
                    kills = parse_sides_stat(stats_tds[4])
                    deaths = parse_sides_stat(stats_tds[5])
                    assists = parse_sides_stat(stats_tds[6])
                    acs = parse_sides_stat(stats_tds[3])
                    kast = parse_sides_stat(stats_tds[8])
                    adr = parse_sides_stat(stats_tds[9])
                    hs = parse_sides_stat(stats_tds[10])
                    first_kills = parse_sides_stat(stats_tds[11])
                    first_deaths = parse_sides_stat(stats_tds[12])

                    # Prepare player data for MongoDB
                    player_data = {
                        "player_id": player_id,
                        "agent": agent_id,
                        "side_data": kills["side_data"],
                        "t_kills": kills["t"],
                        "ct_kills": kills["ct"],
                        "both_kills": kills["both"],
                        "t_deaths": deaths["t"],
                        "ct_deaths": deaths["ct"],
                        "both_deaths": deaths["both"],
                        "t_assists": assists["t"],
                        "ct_assists": assists["ct"],
                        "both_assists": assists["both"],
                        "t_acs": acs["t"],
                        "ct_acs": acs["ct"],
                        "both_acs": acs["both"],
                        "t_kast": f"{kast['t']}%",
                        "ct_kast": f"{kast['ct']}%",
                        "both_kast": f"{kast['both']}%",
                        "t_adr": adr["t"],
                        "ct_adr": adr["ct"],
                        "both_adr": adr["both"],
                        "t_hs": hs["t"],
                        "ct_hs": hs["ct"],
                        "both_hs": hs["both"],
                        "t_first_kills": first_kills["t"],
                        "ct_first_kills": first_kills["ct"],
                        "both_first_kills": first_kills["both"],
                        "t_first_deaths": first_deaths["t"],
                        "ct_first_deaths": first_deaths["ct"],
                        "both_first_deaths": first_deaths["both"]
                    }
                    
                    
                    game_player = insert_or_get_game_player(
                        game_id=game_id,
                        player_id=player_id,
                        team_id=team_id,
                        agent=agent_id,
                        player_role=player_data.get("player_role", None), 
                        side_data=player_data.get("side_data", None), 
                        ct_kills=player_data.get("ct_kills", 0),
                        t_kills=player_data.get("t_kills", 0),
                        both_kills=player_data.get("both_kills", 0),
                        ct_assists=player_data.get("ct_assists", 0),
                        t_assists=player_data.get("t_assists", 0),
                        both_assists=player_data.get("both_assists", 0),
                        ct_deaths=player_data.get("ct_deaths", 0),
                        t_deaths=player_data.get("t_deaths", 0),
                        both_deaths=player_data.get("both_deaths", 0),
                        ct_acs=player_data.get("ct_acs", 0.0),
                        t_acs=player_data.get("t_acs", 0.0),
                        both_acs=player_data.get("both_acs", 0.0),
                        ct_kast=float(player_data.get("ct_kast", "0").strip('%')),  
                        t_kast=float(player_data.get("t_kast", "0").strip('%')), 
                        both_kast=float(player_data.get("both_kast", "0").strip('%')),
                        ct_adr=player_data.get("ct_adr", 0.0),
                        t_adr=player_data.get("t_adr", 0.0),
                        both_adr=player_data.get("both_adr", 0.0),
                        ct_hs=player_data.get("ct_hs", 0.0),
                        t_hs=player_data.get("t_hs", 0.0),
                        both_hs=player_data.get("both_hs", 0.0),
                        ct_first_kills=player_data.get("ct_first_kills", 0),
                        t_first_kills=player_data.get("t_first_kills", 0),
                        both_first_kills=player_data.get("both_first_kills", 0),
                        ct_first_deaths=player_data.get("ct_first_deaths", 0),
                        t_first_deaths=player_data.get("t_first_deaths", 0),
                        both_first_deaths=player_data.get("both_first_deaths", 0)
                    )
                    
                    team_data["players"].append(player_data)

            game_data["teams"].append(team_data)
        
        match["games"].append(game_data)
    
            
def get_tour_split(external_split_id, tour_id, name, link, start_date, end_date, prize_pool, location, parent_region_id):
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
                parent_region_id=parent_region_id,
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
        pp_url= valid_img_url(player_img_url),
        # team_id=team_id,
        region_id=region_id
    )
    session.add(new_player)
    session.commit()
    print(f"Inserted player {player_name} (ID: {player_id}) into PostgreSQL.")

def scrape_split(split_url, tour_id):
    try:
        response = requests.get(split_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        nav_bar = soup.find('div', class_='wf-nav')
        
        nav_items = nav_bar.find_all('a', class_='wf-nav-item')
        
        # Extract the external_split_id from the URL
        parsed_url = urlparse(split_url)
        path_parts = parsed_url.path.strip('/').split('/')
        print(path_parts[1])
        external_split_id = int(path_parts[1]) if len(path_parts) > 1 else None
        
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

        parent_region_id = None
        split_name = split_name.lower()
        for index, word in enumerate(parent_regions):
            if word in split_name:
                parent_region_id = index + 1

        # Get or create the tour split
        split_id = get_tour_split(
            external_split_id=external_split_id,
            tour_id=tour_id,
            name=split_name,
            link=split_url,
            start_date=start_date,
            end_date=end_date,
            prize_pool=prize_pool,
            location=location,
            parent_region_id=parent_region_id
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
        matches = soup_matches.find_all('a', class_='wf-module-item')
        
        for match in matches:
            match_link = match['href'] 
            scrape_game_data(match_link,split_id)
    except Exception as e:
        print(e)
        input()

def scrape_tour_data(tour_url):
    response = requests.get(tour_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find player links
    event_header = soup.find('div', class_='event-header')
    tour_title = event_header.find('div', class_='wf-title').text
    tour_link = tour_url 
    tour_id = get_tour(tour_title.strip(), tour_link)
        
    event_divs = soup.find_all('div', class_='events-container-col')
    events = event_divs[1].find_all('a', class_='wf-card mod-flex event-item')

    for row in events:
        try:
            print("HREF",row['href'])
            split_link = base_url + row['href']
            scrape_split(split_link, tour_id)
        except Exception as e:
            print(e)
            input()

# ----------------------- Main Execution -----------------------

if __name__ == "__main__":
    try:
        for tour_url in all_tours:
            scrape_tour_data(tour_url)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the session when done
        session.close()
        # Close MongoDB connection
        # mongo_client.close()

# Idea: Go through tours 
# get all splits 
# 