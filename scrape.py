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
    
    
def create_player_data(cells):
    player_data = {
        "player_id": cells[0],  
        "agent": cells[1],
        "rating": cells[2],
        "acs": cells[3],
        "kills": cells[4],
        "deaths": cells[5],
        "assists": cells[6],
        "k_d_difference": cells[7],
        "kast": cells[8],
        "adr": cells[9],
        "hs_percentage": cells[10],
        "first_kills": cells[11],
        "first_deaths": cells[12]
    }
    return player_data


def process_team_table(table, team_data, team_index):
    rows = table.find('tbody').find_all('tr')
    team_data[team_index] = []  # Initialize an empty list for team players

    for row in rows:
        # Extract player name and player ID from the first column
        player_td = row.find('td', class_='mod-player')
        player_name = player_td.find('div', class_='text-of').text.strip()
        player_href = player_td.find('a')['href']
        player_id = player_href.split('/')[2]  # Extract the ID from /player/ID/NAME format

        # Add player data to team_data at the correct index (0 for team1, 1 for team2)
        team_data[team_index].append({'player_name': player_name, 'player_id': player_id})
    return team_data

def scrape_game_data(game_url):
    
    # GET MATCHID from URL
    game_id = game_url.split('/')[0]

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
    series = match_header_super.find('div', class_='match-header-event-series')

    # Extract and print the text
    if toury_div:
        text = toury_div.text.strip()
        series = series.text.strip()
        print(f"Tourny: {text}: {series}")
    
    # Find the div with class match-header-vs
    match_header_vs = game_soup.find('div', class_='match-header-vs')
    match_header_vs_score = match_header_vs.find('div', class_='match-header-vs-score')
    
    score_spans = match_header_vs_score.find_all('span')

    # Extract the first and last span
    team1_score = score_spans[0].text.strip()  # First span (team 1 score)
    team2_score = score_spans[-1].text.strip()  # Last span (team 2 score)

    # Extract the match format (Bo3)
    match_format = match_header_vs.find('div', class_='match-header-vs-note').text.strip()

    # Find the first team name (mod-1)
    team1_div = match_header_vs.find('div', class_='match-header-link-name mod-1')
    team1_name = team1_div.find('div', class_='wf-title-med').text.strip()

    # Find the second team name (mod-2)
    team2_div = match_header_vs.find('div', class_='match-header-link-name mod-2')
    team2_name = team2_div.find('div', class_='wf-title-med').text.strip()
        # Print the team names
    print(f"Team 1: {team1_name} {team1_score}")
    print(f"Team 2: {team2_name} {team2_score}")

    if team1_name not in dfs['org'].values:
        # search up team
        team1_link = match_header_vs.find('a',class_='match-header-link wf-link-hover mod-1')
        team2_link = match_header_vs.find('a',class_='match-header-link wf-link-hover mod-1')
        get_org(team1_link)
        get_org(team2_link)
        
    # MAIN GAME STUFF (THIS IS WHAT GOES IN THE DOCUMENTDB)
    main_stats_div = game_soup.find('div', class_='wf-card')
    main_stats_container = main_stats_div.find('div', class_='vm-stats-container')
    vm_stats_games = main_stats_container.find_all('div', class_='vm-stats-game')
    
    maps_arr = []
    for map_div in vm_stats_games:
        
        game_id = map_div['data-game-id']
        
        # takes in all
        map_dict = {"game_id": game_id}
        
        ########################## Header Info #####################################
        game_header_div = map_div.find('div', class_="vm-stats-game-header")
        
        team1_div = game_header_div.find('div', class_='team')
        map_div = game_header_div.find('div', class_='map')
        team2_div = game_header_div.find('div', class_='team mod-right')
        
        # Extract the map name from the span inside the first div
        map_name = map_div.find('span').text.strip()
        map_dict['map_name'] = map_name
        # Extract the duration from the div with class 'map-duration'
        map_duration = map_div.find('div', class_='map-duration').text.strip()
        map_dict['map_duration'] = map_duration
        
        team1_score = team1_div.find('div', class_='score').text.strip()
        team1_name = team1_div.find('div', class_='team-name').text.strip()
        
        team2_score = team2_div.find('div', class_='score').text.strip()
        team2_name = team2_div.find('div', class_='team-name').text.strip()
        
        ########################## Round Info #####################################
        
        rounds = map_div.find_all('div', class_='vlr-rounds-row-col')
        # Initialize an empty list to store the round results
        round_scores = []

        # Loop through the rounds and extract the title attribute (score)
        for round_div in rounds:
            round_title = round_div.get('title')
            if round_title:
                round_scores.append(round_title)

        map_dict['round_scores'] = round_scores
        
        ########################## Player Score Info #####################################
        
        player_scores_tables = map_div.find_all('table', class_='wf-table-inset mod-overview')
        
        team_data = [{'org1_name': team1_name, 'org1_score': team1_score}, {'org2_name': team2_name, 'org2_score': team2_score}]  # Position 0 for team1, Position 1 for team2

        for player_table, player_table_index in player_scores_tables:
            
            player_rows = player_table.find_all('tr')

            for row in player_rows:
                                    
                player_td = row.find('td', class_='mod-player')
                player_href = player_td.find('a')['href']
                player_id = player_href.split('/')[2]  # Extract the ID from /player/ID/NAME format

                row_data_t = [player_id]  
                row_data_ct = [player_id]  
                row_data_both = [player_id] 

                for td in row.find_all('td')[1:]:
                    # Check if the column contains KAST data
                    kast_t = td.find('span', class_='side mod-t').text.strip()
                    row_data_t.append(kast_t)
                    
                    kast_both = td.find('span', class_='side mod-both').text.strip()
                    row_data_both.append(kast_both)
                    
                    kast_ct = td.find('span', class_='side mod-ct').text.strip()
                    row_data_ct.append(kast_ct)
                
                player_data_t = create_player_data(row_data_t)
                player_data_ct = create_player_data(row_data_ct)
                player_data_both = create_player_data(row_data_both)

                team_data[player_table_index].update({'player_data_t': player_data_t, 'player_data_ct': player_data_ct, 'player_data_both': player_data_both})
        
        map_dict['game_data'] = team_data
    
        maps_arr.append(map_dict)
    
    # DECIDE WHAT TO DO WITH THE MAPS
                    
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
                                
            # Append new data to the list
            new_data.append({'player_name': player_name, 'player_url': player_url})

scrape_data()
