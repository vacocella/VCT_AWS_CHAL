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
from urllib.parse import urlparse, parse_qs
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

tour_url = 'https://www.vlr.gg/gc-2024'
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
        # Split the date range
        if '-' in date_str:
            start_str, end_str = date_str.split('-')
            start_str = start_str.strip()
            end_str = end_str.strip()

            # Handle cases where the month is only in the start date
            if ',' not in start_str:
                start_str += f", {end_str.split(',')[-1]}"

            # Parse dates
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
        tour = session.query(Tour).filter_by(name=tour_name).first()
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
        # Ensure you're comparing the column to the value
        tour_split = session.query(Tour_Split).filter(Tour_Split.external_split_id == external_split_id).first()
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
    
def scrape_tour_data():
    response = requests.get(tour_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find player links
    event_header = soup.find('div', class_='event-header')
    tour_title = event_header.find('div', class_='wf-title').text
    tour_link = tour_url 
    tour_id = get_tour(tour_title, tour_link)
        
    events = soup.find_all('a', class_='wf-card mod-flex event-item')

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