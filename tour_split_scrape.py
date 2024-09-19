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
import pdb

# ----------------------- Configuration -----------------------

# Load environment variables from a .env file
load_dotenv()

# Base URL of the website
base_url = 'https://www.vlr.gg'

# PostgreSQL connection string
DATABASE_URL = os.getenv('DATABASE_URL')

# MongoDB connection string
MONGODB_URI = os.getenv('MONGODB_URI')

# NA
event_url = 'https://www.vlr.gg/stats/?event_group_id=all&event_id=2004&series_id=all&region=all&min_rounds=200&min_rating=1550&agent=all&map_id=all&timespan=all'

# China 
event_url = 'https://www.vlr.gg/stats/?event_group_id=all&event_id=2006&series_id=all&region=all&min_rounds=200&min_rating=1550&agent=all&map_id=all&timespan=all'

# EMEA
event_url = 'https://www.vlr.gg/stats/?event_group_id=all&event_id=1998&series_id=all&region=all&min_rounds=200&min_rating=1550&agent=all&map_id=all&timespan=all'

# Pacific
event_url = 'https://www.vlr.gg/stats/?event_group_id=all&event_id=2002&series_id=all&region=all&min_rounds=200&min_rating=1550&agent=all&map_id=all&timespan=all'

# game changers
event_url = 'https://www.vlr.gg/stats/?event_group_id=62&event_id=all&region=all&min_rounds=200&min_rating=1550&agent=all&map_id=all&timespan=all'


tour_url = 'https://www.vlr.gg/vct-2024'

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

# class Player(Base):
#     __tablename__ = 'players'
#     player_id = Column(Integer, primary_key=True)
#     name = Column(String(100))
#     real_name = Column(String(100))
#     pp_url =  Column(String(300))
#     region_id = Column(Integer, ForeignKey('regions.region_id'))
    
class Map(Base):
    __tablename__ = 'maps'
    map_id = Column(Integer, primary_key=True)
    map_name = Column(String(100))
    active = Column(Boolean, default=True)
    
class Tour():
    __tablename__ = 'tour'
    tour_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    link = Column(String(1000))
    
class Tour_Split():
    __tablename__ = 'tour_split'
    external_split_id = Column(Integer,primary_key=True)
    name = Column(String(100))
    link = Column(String(1000))
    start_date = Column(Date)
    end_date = Column(Date)  
    prize_pool = Column(Date)  
    location = Column(String(500))  

# class Match(Base):
#     __tablename__ = 'matches'
#     match_id = Column(Integer, primary_key=True)
#     team1_id = Column(Integer, ForeignKey('teams.team_id'))
#     team2_id = Column(Integer, ForeignKey('teams.team_id'))
#     map_id = Column(Integer, ForeignKey('maps.map_id'))
#     date_played = Column(Date)
#     team1_score = Column(Integer)
#     team2_score = Column(Integer)

#     team1 = relationship('Team', foreign_keys=[team1_id])
#     team2 = relationship('Team', foreign_keys=[team2_id])
#     map = relationship('Map')

# class MatchHistoryPlayer(Base):
#     __tablename__ = 'match_history_player'
#     match_id = Column(Integer, ForeignKey('matches.match_id'), primary_key=True)
#     player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
#     team_id = Column(Integer, ForeignKey('teams.team_id'))
#     agent = Column(String(50))
#     kills = Column(Integer)
#     assists = Column(Integer)
#     deaths = Column(Integer)
#     acs = Column(Float)
#     kast = Column(Float)
#     adr = Column(Float)
#     first_kills = Column(Integer)
#     first_deaths = Column(Integer)

#     match = relationship('Match')
#     player = relationship('Player')
#     team = relationship('Team')

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


def scrape_split(split_url):
    response = requests.get(split_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    

def scrape_tour_data():
    response = requests.get(tour_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find player links
    event_header = soup.find('div', class_='event-header')
    tour_title = event_header.find('div', class_='wf-title').text
    
    events = soup.find_all('a', class_='wf-card mod-flex event-item')

    for row in events:
        try:
            print(row['href'])
            scrape_split(base_url + row['href'])
        except e:
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