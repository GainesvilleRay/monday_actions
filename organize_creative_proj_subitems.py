# import the modules
import datetime
import json
import pandas as pd
 
#access file of json data
dir_path = r'/Users/douglasray/Projects/monday_actions/'
json_data = 'data_files/creative_projects/2023_creative_projects_subitems_merged.json'
data_file = dir_path + json_data

def build_lists(data_file):
    """
    Build several lists: 
    action_dict_list is a list of each action
    created_events is a list of actions where pulse is created
    archved_events is a list of actions were pulse is archived
    pulse_ids is a list of unique pulse_id values for all actions
    """
    
    #read json data
    with open(data_file) as json_file:
        data = json.load(json_file)
    
    action_dict_list = [] # Build list of dictionaries for each action
    created_events = [] # Separate list for actions where pulse is created
    archived_events = [] # Separate list for actions were pulse is archived
    pulse_ids = [] # Build list of the pulse_id values


    for item in data:
        item_dict = {} # Build dictionary for each action
        item_dict['event'] = item['event']
        item['created_at'] = int(item['created_at'])/10000000 # Several steps to convert unix string to datatime obj
        item['created_at'] = datetime.datetime.fromtimestamp(item['created_at']).date() # Convert to datetime date
        item_dict['action_date'] = item['created_at']
        data_level = json.loads(item['data']) # Normalize level that comes as jason string
        item['data'] = data_level
        if 'create_pulse' in item.values():
            item_dict['created_date'] = item['created_at']
        if 'group_id' in data_level.keys():
            item_dict['unit_id'] = data_level['group_id']
        if 'pulse_id' in data_level.keys():
            item_dict['pulse_id'] = data_level['pulse_id']
        if 'pulse_name' in data_level.keys():
            item_dict['pulse_name'] = data_level['pulse_name']
        if 'parent_item_id' in data_level.keys():
            item_dict['parent_id'] = data_level['parent_item_id']
        if 'item_name' in data_level.keys():
            item_dict['item_name'] = data_level['item_name']
        if 'archive_pulse' in item.values():
            item_dict['archive_date'] = item['created_at']
        action_dict_list. append(item_dict) # return
        if 'created_date' in item_dict.keys():
            created_events.append(item_dict) # return
        if 'archive_date' in item_dict.keys():
            archived_events.append(item_dict) # return
    for item in action_dict_list:
        if 'pulse_id' in item.keys():
            pulse_ids.append(item['pulse_id'])
    pulse_ids = list(set(pulse_ids)) # removed duplicates; return
    return action_dict_list, created_events, archived_events, pulse_ids

def merge_created_with_archived(created_events, archived_events, pulse_id):
    new_dict = {}
    for item in created_events:
        if item['pulse_id'] == pulse_id:
            new_dict['pulse_id'] = pulse_id
            new_dict['pulse_name'] = item['pulse_name']
            new_dict['unit_id'] = item['unit_id']
            new_dict['created_date'] = item['created_date']
            if 'parent_id' in item.keys():
                new_dict['parent_id'] = item['parent_id']
        if 'item_name' in item.keys():
            new_dict['item_name'] = item['item_name']
    for elem in archived_events:
        if elem['pulse_id'] == pulse_id:
            if 'archive_date' in elem.keys():
                new_dict['archive_date'] = elem['archive_date']
    return new_dict

if __name__ == "__main__":
    action_dict_list, created_events, archived_events, pulse_ids = build_lists(data_file)
    action_dicts = []
    for pulse_id in pulse_ids:
        new_dict = merge_created_with_archived(created_events, archived_events, pulse_id)
        action_dicts.append(new_dict)
    df = pd.DataFrame(action_dicts)
    df.to_excel('creative_projects_subitems.xlsx')