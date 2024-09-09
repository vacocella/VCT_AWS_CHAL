import requests
from bs4 import BeautifulSoup
import pandas as pd


player_df = pd.DataFrame(columns=['player_name', 'player_url'])
org_df = pd.DataFrame(columns=['org_name', 'org_url'])

dfs = {'org': org_df, 'player': player_df}

# Example URL to scrape
url = 'https://www.vlr.gg'  # Replace with the actual target URL


def get_org(org_link):
                        
    team1_res = requests.get(url + org_link.get('href'))
    team1_req_soup = BeautifulSoup(team1_res.content, 'html.parser')
    
    team_nation = team1_req_soup.find('div', class_='team-header')
    nation = team_nation.find('div',class_='team-header-country').get_text(strip=True)
    print(nation)


def scrape_game_data(game_url):
    game_response = requests.get(url + game_url)
    game_soup = BeautifulSoup(game_response.content, 'html.parser')
    
    # Find the div with class match-header-super
    match_header_super = game_soup.find('div', class_='match-header-super')
    
    # Extract the date from the first 'moment-tz-convert' div
    date = match_header_super.find('div', {'data-moment-format': 'dddd, MMMM Do'}).text.strip()

    # Extract the time from the second 'moment-tz-convert' div
    time = match_header_super.find('div', {'data-moment-format': 'h:mm A z'}).text.strip()

    # Extract the patch from the div with style 'font-style: italic'
    patch = match_header_super.find('div', style='font-style: italic;').text.strip()

    # Find the div with the style font-weight: 700
    toury_div = match_header_super.find('div', style='font-weight: 700;')
    series = match_header_super.find('div', style='match-header-event-series')

    # Extract and print the text
    if toury_div:
        text = toury_div.text.strip()
        series = series.text.strip()
        print(f"Tourny: {text}: {series}")
    input()
    
    # Find the div with class match-header-vs
    match_header_vs = game_soup.find('div', class_='match-header-vs')

    # Find the first team name (mod-1)
    team1_div = match_header_vs.find('div', class_='match-header-link-name mod-1')
    team1_name = team1_div.find('div', class_='wf-title-med').text.strip()

    # Find the second team name (mod-2)
    team2_div = match_header_vs.find('div', class_='match-header-link-name mod-2')
    team2_name = team2_div.find('div', class_='wf-title-med').text.strip()
        # Print the team names
    print(f"Team 1: {team1_name}")
    print(f"Team 2: {team2_name}")

    if team1_name not in dfs['org'].values:
        # search up team
        team1_link = match_header_vs.find('a',class_='match-header-link wf-link-hover mod-1')
        team2_link = match_header_vs.find('a',class_='match-header-link wf-link-hover mod-1')
        get_org(team1_link)
        get_org(team2_link)
        
        
    # MAIN GAME STUFF
    main_stats_div = game_soup.find('div', class_='wf-card')
    main_stats_container = main_stats_div.find('div', class_='vm-stats-container')
    vm_stats_games = main_stats_container.find_all('div', class_='vm-stats-game')
    
    maps = {}
    for map_div in vm_stats_games:
        
        game_id = map_div['data-game-id']
        if game_id != "all":
            team1_div = map_div.find('div', class_='team')
            map_div = team1_div.find('div', class_='map')

            # Extract the map name from the span inside the first div
            map_name = map_div.find('span').text.strip()

            # Extract the duration from the div with class 'map-duration'
            map_duration = map_div.find('div', class_='map-duration').text.strip()
            
            team1_score = team1_div.find('div', class_='score').text.strip()
            team1_name = team1_div.find('div', class_='team-name').text.strip()
            
            
        
        
    
        
    
    

def scrape_player_page(player_url):
    
    internal_response = requests.get(url + player_url)
    internal_soup = BeautifulSoup(internal_response.content, 'html.parser')
    
        # Find the div with class col-container
    container = internal_soup.find('div', class_='col-container')

    # Find the player-header div
    player_header = container.find('div', class_='player-header')

    # Extract the player's name from the h1 tag
    player_name = player_header.find('h1', class_='wf-title').text.strip()

    # Extract the player's real name from the h2 tag (if it exists)
    player_real_name = player_header.find('h2', class_='player-real-name').text.strip()

    # Extract the nationality from the div containing the flag and country name
    nationality_div = player_header.find('div', class_='ge-text-light')
    nationality = nationality_div.text.strip()
    print(player_name,player_real_name, nationality)
    
    mod_dark_div = container.find('div', class_='mod-dark')
    
    divs = mod_dark_div.find_all('div')
    
    # Loop through all the games
    for div in divs:
        a_tag = div.find('a')
        if a_tag and a_tag['href']:
            # Print the a tag link
            scrape_game_data(a_tag['href'])



def scrape_data():
    # Send request to the website
    response = requests.get(url + '/stats')
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the wf-card mod-table mod-dark div and loop through rows
    table = soup.find('div', class_='wf-card mod-table mod-dark')
    rows = table.find('tbody').find_all('tr')

    # Create a list to store new rows
    new_data = []

    # Iterate through rows and extract player data
    for row in rows:
        player_td = row.find('td', class_='mod-player mod-a')
        if player_td:
            # Extract player name and URL
            player_name = player_td.find('div', class_='text-of').text.strip()
            player_url = player_td.find('a')['href'].replace('/player/', '/player/matches/')
            
            scrape_player_page(player_url)
                                
            input()
            # Append new data to the list
            new_data.append({'player_name': player_name, 'player_url': player_url})

scrape_data()
