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

class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    real_name = Column(String(100))
    pp_url =  Column(String(300))
    region_id = Column(Integer, ForeignKey('regions.region_id'))

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

def scrape_data():
    response = requests.get(event_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find player links
    table = soup.find('div', class_='wf-card mod-table mod-dark')
    tbody = table.find('tbody') if table else None
    rows = tbody.find_all('tr') if tbody else []

    for row in rows:
        try:
            player_td = row.find('td', class_='mod-player mod-a')
            if player_td:
                player_url = player_td.find('a')['href']
                scrape_player_page(player_url)

                    # time.sleep(1)  # Delay between requests
        except e:
            print(e)

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



# Idea: Go through tours 
# get all splits 
# 