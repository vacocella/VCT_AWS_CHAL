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

def scrape_tour_data(tour_url):
    response = requests.get(tour_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    for row in events:
        try:
            print("HREF",row['href'])
            split_link = base_url + row['href']
        except Exception as e:
            print(e)

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
