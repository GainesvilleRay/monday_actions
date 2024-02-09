"""
Functions here are for three separate jobs.
First function just gets info from Monday.com to identify board names and id numbers.
Second and third functions work together to query data for activities on a given board,
for a given time frame, and then write that to a json file.
Fourth function merges those json files for different time frames into a single json file
for a given board. Developed by Douglas Ray in early 2024.

"""

import json
import requests
 
# Update the apiKey as needed here:
apiKey = "" #<<< PUT KEY HERE 
headers = {"Authorization" : apiKey}
apiUrl = "https://api.monday.com/v2"

def find_board_info():
    """
    This gathers a list of dictionaries that show the name and id num for each
    board on monday.com maintained by CLAS Media Services. This info can be used
    for other queries, such as for activity logs.
    """
    apiKey = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI5Njk3MzgwOCwiYWFpIjoxMSwidWlkIjozNjIwNzY1MywiaWFkIjoiMjAyMy0xMS0xN1QxNDoxMTo1MC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NjM4NTU5MCwicmduIjoidXNlMSJ9.eYc7Q6gig_cnjWcoV6W5RxBCNYUZI17dJtAjCN8DMQo"
    headers = {"Authorization" : apiKey}
    apiUrl = "https://api.monday.com/v2"
    boards_query = f'query {{ boards (limit:100) {{ name id }}}}'
    data = {'query' : boards_query}
    r = requests.post(url=apiUrl, json=data, headers=headers)
    boards_result = r.json()
    boards_dicts = boards_result['data']['boards'] # List of dicts for each board
    board_ids = [] # List of board id numbers
    for dict in boards_dicts:
        board_ids.append(dict['id'])
    return boards_dicts, board_ids

def group_board_info():
    """
    This gathers a list of dictionaries that show the name and id num for each
    group on a given monday.com maintained by CLAS Media Services.
    """
    apiKey = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI5Njk3MzgwOCwiYWFpIjoxMSwidWlkIjozNjIwNzY1MywiaWFkIjoiMjAyMy0xMS0xN1QxNDoxMTo1MC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NjM4NTU5MCwicmduIjoidXNlMSJ9.eYc7Q6gig_cnjWcoV6W5RxBCNYUZI17dJtAjCN8DMQo"
    headers = {"Authorization" : apiKey}
    apiUrl = "https://api.monday.com/v2"
    groups_query = f'query {{ boards (ids:592284362) { groups { id, title }}}}'
    boards_query = f'query {{ boards (limit:100) {{ name id }}}}'
    data = {'query' : groups_query}
    r = requests.post(url=apiUrl, json=data, headers=headers)
    groups_result = r.json()
    groups_dicts = groups_result['data']['boards'] # List of dicts for each board
    #group_ids = [] # List of board id numbers
    #for dict in boards_dicts:
    #    board_ids.append(dict['id'])
    return groups_dicts

groups_dicts = find_group_info()
for item in groups_dicts:
    print(item)
    
def activity_query(board_id, from_date, to_date):
    """
    This gathers a list of dictionaries for each activity by CLAS Media Services
    on a given monday.com board.
    """
    apiKey = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI5Njk3MzgwOCwiYWFpIjoxMSwidWlkIjozNjIwNzY1MywiaWFkIjoiMjAyMy0xMS0xN1QxNDoxMTo1MC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NjM4NTU5MCwicmduIjoidXNlMSJ9.eYc7Q6gig_cnjWcoV6W5RxBCNYUZI17dJtAjCN8DMQo"
    headers = {"Authorization" : apiKey}
    apiUrl = "https://api.monday.com/v2"
    #query = f'query {{ boards (ids: {board_id}) {{ activity_logs (from: \"{from_date}", to: \"{to_date}", limit:10000 {{ id event data entity user_id created_at }}}}}}'
    query = f'query {{ boards (ids: {board_id}) {{ activity_logs (from: \"{from_date}", to: \"{to_date}", limit:10000) {{ id event data entity user_id created_at }}}}}}'
    data = {'query' : query}
    r = requests.post(url=apiUrl, json=data, headers=headers) # make request
    activity_result = r.json()
    #if 'errors' in activity_result:
    #    print('Found an error:\n' + str(activity_result))
    #else:
    dict_for_each_activity = activity_result['data']['boards'][0]['activity_logs']
    #if len(dict_for_each_activity) > 9999:
    #       print("More records found than available in query limit.")
    #else:
    print(len(dict_for_each_activity))
    return dict_for_each_activity

def write_to_json(dir_path, activity_result, board_name, year, quarter):
    
    json_object = json.dumps(activity_result, indent=4)
    json_data = f'{board_name}_{year}{quarter}.json'
    data_file = dir_path + json_data
    with open(data_file, "w") as outfile:
        outfile.write(json_object)

def merge_JsonFiles(filename, newfile):
    
    result = list()
    for f1 in filename:
        with open(f1, 'r') as infile:
            result.extend(json.load(infile))

    with open(newfile, 'w') as output_file:
        json.dump(result, output_file)

"""
###### QUERY MONDAY.COM FOR ACTIVITY AND WRITE TO JSON FILES #####
# Limit is capped at 10000 per query
limit = 10000
# Date ranges
year = '2023'
q1_start = f'{year}-01-01T00:00:00Z'
q1_end = f'{year}-03-31T23:59:59Z'
q2_start = f'{year}-04-01T00:00:00Z'
q2_end = f'{year}-06-30T23:59:59Z'
q3_start = f'{year}-07-01T00:00:00Z'
q3_end = f'{year}-09-30T23:59:59Z'
q4_start = f'{year}-10-01T00:00:00Z'
q4_end = f'{year}-12-31T23:59:59Z'
# Adjust per query
quarter = 'q4'
from_date = q4_start
to_date = q4_end

# Select board name and number from dict below:
board_name = 'events' 
board_id = 736608574
dir_path = r'/Users/douglasray/Projects/monday_actions/data_files/events/'

# Use this to get json files of activity on a given board:
dict_for_each_activity = activity_query(board_id, from_date, to_date)
write_to_json(dir_path, dict_for_each_activity, board_name, year, quarter)
"""

"""
# ##### MERGE FILES #######
# To merge files, uncomment and update these with the file names to be merged:
dir_path = r'/Users/douglasray/Projects/monday_actions/data_files/events/'
file1 = dir_path + 'events_2021q1.json'
file2 = dir_path + 'events_2021q2.json'
file3 = dir_path + 'events_2021q3.json'
file4 = dir_path + 'events_2021q4.json'
file5 = dir_path + 'events_2022q1.json'
file6 = dir_path + 'events_2022q2.json'
file7 = dir_path + 'events_2022q3.json'
file8 = dir_path + 'events_2022q4.json'
file9 = dir_path + 'events_2023q1.json'
file10 = dir_path + 'events_2023q2.json'
file11 = dir_path + 'events_2023q3.json'
file12 = dir_path + 'events_2023q4.json'
newfile = dir_path + '2021-23_events_merged.json'
files = [file1, file2, file3, file4, file5, file6, file7, file8, file9, file10, file11, file12]
merge_JsonFiles(files, newfile)
"""

"""
# ##### GET BOARD INFO ######
# Call function to get info on monday.com boards
boards_dicts, board_ids = find_board_info()

# This info from the query on board info. Using boards with team activity:
selected_boards_dicts = [
     {'name': 'Weekly Assignment Board', 'id': '4177388823'}, # nothing there
     {'name': 'Subitems of CLAS Event Calendar', 'id': '3222726524'}, # nothing after 02/03/2023
     {'name': 'Subitems of Ytori', 'id': '2684467964'}, # yes
     {'name': 'Subitems of CLAS Communication Projects', 'id': '2443727631'}, # yes
     {'name': 'Social Media', 'id': '1535106374'}, # yes
     {'name': 'Ytori', 'id': '1117121066'}, # yes
     {'name': 'CLAS Event Calendar', 'id': '736608574'}, # yes
     {'name': 'Digital Content', 'id': '623119076'}, # yes
     {'name': 'CLAS Communication Projects', 'id': '592284362'}
     ]

print(board_ids)
"""

groups_dicts = group_board_info()
