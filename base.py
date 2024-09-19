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


class Map(Base):
    __tablename__ = 'maps'
    map_id = Column(Integer, primary_key=True)
    map_name = Column(String(100))
    active = Column(Boolean, default=True)
    