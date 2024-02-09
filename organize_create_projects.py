"""
This is part of a larger effort to analyze team efforts by the CLAS Media Services staff, 
which uses Monday.com to organize different kinds of projects. A separate script gathers
that data on activities into json files. 
This script takes a json file with data on activities from our board on Monday.com for creative projects,
organizes these actions by the projects they involve, and does some initial analysis
to find things like how long it took us to complete each project, how many individual actions
were involved, and deadline performance. Some of that analysis will vary from board to board. 
Output is an Excel file to allow further analysis. This was developed in early 2024 by
Douglas Ray.
"""

# Python built-in modules
from collections import OrderedDict
from collections.abc import MutableMapping
import datetime
import flatdict
import json
import requests

# Imported modules
import pandas as pd

data_file = '/Users/douglasray/Projects/monday_actions/data_files/creative_projects/2021-23_creative_projects_merged.json'

units_dict = {
    'new_group82157': 'Academic Advising Center', 
    'new_group64256': 'African American Studies Program', 
    'topics': 'Advancement', 
    'new_group31526': 'Anthropology', 
    'new_group98531': 'Artificial Intelligence Initiatives', 
    'new_group8972': 'Archie Carr Sea Turtle Research Center', 
    'new_group34033': 'Astronomy',
    'new_group12899': 'Beyond120',
    'new_group4108': 'Biology',
    'new_group91025': 'Bob Graham Center',
    'new_group83765': 'Bureau of Economic & Business Research (BEBR)',
    'new_group93946': 'CLAS Academic Resources', 
    'new_group37395': 'Center for African Studies',
    'new_group57361': 'Center for European Studies', 
    'new_group35372': 'Deans Office', 
    'new_group83697': 'Internal', 
    'new_group38741': 'Environmental Design', 
    'new_group73404': 'Shorstein Center for Jewish Studies', 
    'new_group65216': 'Chemistry', 
    'new_group78440': 'CLAS Newsletter - Sent Every Other Wednesday', 
    'new_group12736': 'Classics Department', 
    'new_group91513': 'CLAS Student Exchange', 
    'new_group31476': 'Dial Center', 
    'new_group70641': 'Department of Gender, Sexuality, and Womens Studies', 
    'new_group17064': 'Economics', 
    'new_group29192': 'English', 
    'new_group21720': 'English Language Institute', 
    'new_group98860': 'Geography', 
    'new_group72451': 'Geological Sciences', 
    'new_group1091': 'History',
    'new_group53755': 'Humanities Department', 
    'new_group58014': 'Languages, Literatures, and Cultures', 
    'new_group7787': 'Linguistics',
    'mathematics_department': 'Mathematics Department', 
    'new_group94612': 'Center for Medieval and Early Modern Studies', 
    'new_group78691': 'Philosophy', 
    'new_group96707': 'Physics', 
    'new_group30786': 'Political Science', 
    'new_group3634': 'Psychology', 
    'new_group42110': 'Religion', 
    'new_group68372': 'Shared Services', 
    'new_group1816': 'Samuel Proctor Oral History Program', 
    'new_group75729': 'Sociology, Criminology & Law', 
    'new_group44359': 'Spanish and Portuguese Studies', 
    'new_group41599': 'Statistics',
    'new_group26324': 'Sustainability Studies Department', 
    'new_group90039': 'University Writing Program'
    }

def flatten_dict(elem: MutableMapping, sep: str= '.') -> MutableMapping:
    # Flattens dict of dicts for each action
    [flat_dict] = pd.json_normalize(elem, sep=sep).to_dict(orient='records')
    return flat_dict

def flatten_list(new_keys_list):
    """ 
    Flattens list of lists of keys, and removes duplicates. 
    """
    all_keys_list = [item for row in new_keys_list for item in row]
    unique_keys_list = list(OrderedDict.fromkeys(all_keys_list))
    return unique_keys_list

def make_activity_list(data_file, units_dict):
    """
    Loop through list of dicts for all activity and flatten them.
    Get keys for all the dicts, and make a list of unique keys.
    """
    # Opening JSON file
    f = open(data_file)
    data = json.load(f)

    list_activity_dicts = [] # Will become a list of dicts for each activity on the monday.com board
    list_activity_keys = [] # Keys from that list that we'll eventually swap out
    # Drill down a few layers in the json data to expose the dicts of dicts we want to keep
    for elem in data:
        data_level = json.loads(elem['data']) # Normalize level that comes as jason string
        elem['data'] = data_level
        flat_dict = flatten_dict(elem) # Flatten nested dicts
        flat_dict['created_at'] = int(flat_dict['created_at'])/10000000 # Several steps to convert unix string to datatime obj
        flat_dict['created_at'] = datetime.datetime.fromtimestamp(flat_dict['created_at']).date() # Convert to datetime date
        if 'data.previous_value.date' in flat_dict:  #Several steps to convert string to datetime
            old_string_date1 = flat_dict['data.previous_value.date']
            if len(old_string_date1) > 11: # Some of these dates include time as well as date, so trim that
                old_string_date1 = old_string_date1.split("T")[0]
            converted_old_date1 = datetime.datetime.strptime(old_string_date1,'%Y-%m-%d').date()
            flat_dict['data.previous_value.date'] = converted_old_date1
        if 'data.value.date' in flat_dict:  #Several steps to convert string to datetime
            old_string_date2 = flat_dict['data.value.date']
            if len(old_string_date2) > 11: # Some of these dates include time as well as date, so trim that
                old_string_date2 = old_string_date2.split("T")[0]
            converted_old_date2 = datetime.datetime.strptime(old_string_date2,'%Y-%m-%d').date()
            flat_dict['data.value.date'] = converted_old_date2
        if 'data.previous_value.changed_at' in flat_dict:  #Several steps to convert string to datetime
            old_string_date3 = flat_dict['data.previous_value.changed_at']
            if len(old_string_date3) > 11: # Some of these dates include time as well as date, so trim that
                old_string_date3 = old_string_date3.split("T")[0]
            converted_old_date3 = datetime.datetime.strptime(old_string_date3,'%Y-%m-%d').date()
            flat_dict['data.previous_value.changed_at'] = converted_old_date3
        if 'data.group_id' in flat_dict:
            sub = flat_dict['data.group_id']
            flat_dict['data.group_name'] = units_dict.get(sub)
        elem_keys = list(flat_dict.keys()) # Flatten nested keys
        list_activity_keys.append(elem_keys) # Make list of lists for all the keys
        list_activity_dicts.append(flat_dict) # Make list of dicts for all the activities
    return list_activity_keys, list_activity_dicts

def map_keys():
    """
    Create a dict of monday.com keys mapped to new names for these.
    """
    key_map = {
        'id':'action_id',
        'event':'event',
        'entity':'item_type',
        'user_id':'user_id',
        'created_at':'event_date',
        'data.board_id':'board_id',
        'data.group_id':'group_id',
        'data.pulse_id':'pulse_id',
        'data.pulse_name':'pulse_name',
        'data.column_id':'column_id',
        'data.column_title':'column_name',
        'data.value.date':'new_date',
        'data.previous_value':'prior_value',
        'data.item_id':'item_id',
        'data.item_name':'item_name',
        'data.item_type':'item_type',
        #'data.value.personsAndTeams':'team_member', << Values for this are lists
        'data.textual_value':'new_text',
        'data.previous_value.changed_at':'date_last_changed',
        'data.previous_textual_value':'prior_text',
        'data.value.label.text':'value_text',
        'data.value.label.is_done':'is_done',  # << Values for this are boolean
        #'data.value.chosenValues':'value_chosen', << Values for this are lists
        'data.group_name':'group_name',
        'data.previous_value.label.text':'prior_value_text',
        'data.previous_value.date':'prior_value_date',
        #'data.value.linkedPulseIds':'linked_pulse_ids', << Values for this are lists
        'data.value.name':'value_name',
        'data.previous_value.name':'prior_value_name',
        'data.source_board.id': 'source_board_id',
        'data.source_board.name':'source_board_name',
        'data.dest_board.id':'destination_board_id',
        'data.dest_board.name':'destination_board_name',
        'data.source_group.id':'source_group_id',
        'data.source_group.title':'source_group_title',
        'data.dest_group.id':'destination_group_id',
        'data.dest_group.title':'destination_group_title',
        'data.pulse.id':'pulse_id',
        'data.pulse.name':'pulse_name',
        'data.group_title':'group_title',
        'data.source_pulse_id':'source_pulse_id',
        'data.dest_pulse_id':'destination_pulse_id',
        'data.source_board_id':'source_board_id',
        'data.dest_board_id':'destination_board_id',
    }
    #Make a list of the keys we want to keep
    my_keys_list = list(key_map.keys())
    
    return key_map, my_keys_list

def checkKey(list_activity_dicts, my_keys_list, key_map):
    """
    Initialize and populate a dictionary of all activities for a given board
    that only includes the key:value pairs we want to keep, and renames keys.
    """
    new_list = []
    for dict in list_activity_dicts:
        new_dict = {}
        for key in dict.keys(): # Check for key in given dict
            if key in my_keys_list: # If it's there add key:value to new dict
                new_dict[key] = dict[key]
        new_dict = {key_map[k]: v for k, v in new_dict.items()} # Swap new keys for old    
        new_list.append(new_dict) # Add the new dict to a list of dicts
    
    return new_list

def check_for_pulses(new_list):
    """
    Selecting for things that involve projects ('pulses'),
    not for creating, renaming or archiving boards.
    """
    pulse_list = []
    for elem in new_list:
        if 'pulse_id' in elem.keys():
            pulse_list.append(elem)
        else:
            continue
            
    return pulse_list

def create_unique_pulse_id_list(pulse_list):
    """
    Create a list of unique pulse ids
    """
    pulse_id_list = []
    for item in pulse_list:
        pulse_id = item['pulse_id']
        pulse_id_list.append(pulse_id)
    unique_pulse_ids = list(dict.fromkeys(pulse_id_list))
    
    return unique_pulse_ids

def gather_dicts_by_pulse(unique_pulse_ids, pulse_list):
    """
    Organize the list of dicts for all actions by pulse id,
    creating a list of lists of dictionaries. Each project
    is its own list of dictionaries of related actions.
    """
    #project_actions = []
    gathered_projects = []
    for pulse_id in unique_pulse_ids: # Loop through all pulse_id values ...
        project_actions = []
        for item in pulse_list: # Compare with each item in the list of all projects
            if item["pulse_id"] == pulse_id: # If it has the pulse_id we're seeking ...
                project_actions.append(item) # add it a list for that pulse_id
        gathered_projects.append(project_actions) # And then add that list to a list for all projects
        
    return gathered_projects
    
"""
Next up: organize projects in other ways: by project type, unit, etc.
Evaluate which ones were open the longest, had the most delays, etc.

To evaluate by project type: 'column_name':'Type of Work' + 'new_text': 'Photography'
"""

def get_group_name(id):
    """
    Gets CLAS unit name based on monday.com group_id
    """
    board_file = '/Users/douglasray/Projects/monday_actions/data_files/board_id.json'
    f = open(board_file)
    data = json.load(f)
    group_dicts = data['data']['boards'][0]['groups']
    for i in group_dicts:
        if id in i.values():
            unit_name = i['title']
    return unit_name

def flatten(project_types):
    """
    Function for cleaning up work types, that are sometimes listed singularly
    and sometimes several in a single string. Used in build_filtered_projects, below.
    """
    project_types = [i.split(',') for i in project_types] # Break up strings that contain a comma
    project_types = [item for row in project_types for item in row] # Gather items into a list of lists
    # Flatten list of lists of strings
    wt = []
    for i in project_types:
        if isinstance(i,list): wt.extend(flatten(i))
        else: wt.append(i)
    wt = list(set(wt))
    wt = [x.strip(' ') for x in wt]
    return wt

def build_filtered_projects(gathered_projects):
    """
    Takes list of lists of dicts for all pulse projects and builds a simpler list
    of dicts with selected key:value pairs. Maybe the final step.
    """
    filtered_projects = []
    for i in gathered_projects: # loop through projects as items in list of projects
        project_dict = {}
        project_dict['pulse_id'] = i[0]['pulse_id']
        project_dict['total_actions'] = len(i)
        project_dict['archive_date'] = "Not archived"
        archive_dates = []
        deadline_updates = []
        project_types = []
        for e in i: #loop though the items in a given project
            if 'pulse_name' in e.keys():
                project_dict['pulse_name'] = e['pulse_name']
            if 'group_id' in e.keys():
                project_dict['group_id'] = e['group_id']
            if 'group_name' in e.keys():
                project_dict['group_name'] = e['group_name']
            if 'create_pulse' in e.values():
                project_dict['created'] = e['event_date']
            if 'Type of Work' in e.values():
                if 'new_text' in e.keys():
                    project_type = e['new_text']
                    project_types.append(project_type)
            if 'Internal Due Date' in e.values():
                if 'new_date' in e.keys():
                    deadline = e['new_date']
                    deadline_updates.append(deadline)
                else:
                    deadline = "removed"
            if 'archive_pulse' in e.values():
                archive_date = e['event_date']
                archive_dates.append(archive_date)
        if len(deadline_updates) == 0:
            project_dict['due_date'] = "No deadline set"
        elif len(deadline_updates) == 1:
            project_dict['due_date'] = deadline_updates[0]
        else:
            project_dict['due_date'] = deadline_updates
        if len(archive_dates) == 0:
            project_dict['archive_date'] = "Not archived"
        elif len(archive_dates) == 1:
            project_dict['archive_date'] = archive_dates[0]
        else:
            project_dict['archive_date'] = archive_dates
        if len(project_types) == 0:
            project_dict['project_type'] = "undetermined"
        elif len(project_types) == 1:
            project_dict['project_type'] = project_types[0]
        else:
            project_dict['project_type'] = flatten(project_types)
        filtered_projects.append(project_dict)
    return filtered_projects

def define_project_types(filtered_projects):
    """
    The values for project types come in as a list if more than one project type is specified,
    which happens often. This separates them as separate keys with Boolean values.
    """

    filtered_projects2 = []
    project_types = [  # For reference, not for use
        "Editorial",
        "Email",
        "Email Marketing",
        "Event Promotion",
        "Graphic Design",
        "News & Pubs",
        "Photography",
        "Print Production",
        "Social Media Marketing",
        "Staff",
        "Videography",
        "Web (People)"
        ]

    for elem in filtered_projects:
        if 'created' in elem.keys(): 
            if isinstance(elem['archive_date'], list):
                youngest = min(dt for dt in elem['archive_date'])
                elem['archive_date'] = youngest
            if isinstance(elem['archive_date'], str):
                pass
            else:
                elem['lifespan'] = abs(elem['archive_date'] - elem['created']).days
        if 'due_date' in elem.keys():
            if isinstance(elem['due_date'], list):
                d = len(elem['due_date'])
                d1 = len(elem['due_date']) # Saving this so we can use in delay calc
                elem['due_date'].sort()
                while d > 0:
                    elem[f'delay{d-1}'] = elem['due_date'][d-1]
                    d = d - 1
                if isinstance(elem['archive_date'], str):
                    pass
                else:
                    elem['past_due'] = abs(elem[f'delay{d1-1}'] - elem['archive_date']).days
            elif isinstance(elem['due_date'], str):
                pass
            else:
                if isinstance(elem['archive_date'], str):
                    pass
                else:
                    elem['past_due'] = abs(elem['due_date'] - elem['archive_date']).days
        # Project types separated bools as often there are more than one type per project
        if "Editorial" in elem["project_type"]:
                elem["type_editorial"] = True
        else:
            elem["type_editorial"] = False
        if "Email" in elem["project_type"]:
                elem["type_email"] = True
        else:
            elem["type_email"] = False
        if "Email Marketing" in elem["project_type"]:
                elem["type_email_marketing"] = True
        else:
            elem["type_email_marketing"] = False
        if elem["type_email_marketing"] == True: # condensing "Email" and "Email Marketing"
            elem["type_email"] = True
        #elem.pop(elem["type_email_marketing"]) # Apparently key doesn't exist in all projects?
        if "Event Promotion" in elem["project_type"]:
                elem["type_event"] = True
        else:
            elem["type_event"] = False
        if "Graphic Design" in elem["project_type"]:
                elem["type_design"] = True
        else:
            elem["type_design"] = False
        if "News & Pubs" in elem["project_type"]:
                elem["type_news"] = True
        else:
            elem["type_news"] = False
        if "Photography" in elem["project_type"]:
                elem["type_photo"] = True
        else:
            elem["type_photo"] = False
        if "Print Production" in elem["project_type"]:
                elem["type_print"] = True
        else:
            elem["type_print"] = False
        if "Social Media Marketing" in elem["project_type"]:
                elem["type_social"] = True
        else:
            elem["type_social"] = False
        if "Staff" in elem["project_type"]:
                elem["type_staff"] = True
        else:
            elem["type_staff"] = False
        if "Videography" in elem["project_type"]:
                elem["type_video"] = True
        else:
            elem["type_video"] = False
        if "Web (People)" in elem["project_type"]:
                elem["type_web_people"] = True
        else:
            elem["type_web_people"] = False
        if "Web" in elem["project_type"]:
                elem["type_web"] = True
        else:
            elem["type_web"] = False
        if elem["type_web_people"] == True: #condensing "Web (People)" and "Web"
            elem['web'] = True
        #elem.pop(elem["type_web_people"]) # Apparently key doesn't exist in all projects?
        filtered_projects2.append(elem)
    return filtered_projects2

def build_filtered_df(filtered_projects):
    """"
    Make dataframe of dicts for created projects, group them by unit,
    and export an excel file that counts them by unit.
    """
    #cols = ['pulse_name', 'pulse_id', 'group_name', 'group_id', 'project_type', 'created', 'due_date', 'archive_date', 'total_actions']
    df_filtered = pd.DataFrame(filtered_projects)
    #df_filtered = df_filtered[cols]
    return df_filtered

if __name__ == "__main__":
    # Run all these things
    list_activity_keys, list_activity_dicts = make_activity_list(data_file, units_dict) # Create list of dicts for all activity, mostly pulses; and their keys
    unique_keys_list = flatten_list(list_activity_keys) # Make single list of all the unique keys
    key_map, my_keys_list = map_keys() # key_map is a dict of original keys with values for new key names; the list is of the old key names
    new_list = checkKey(list_activity_dicts, my_keys_list, key_map) # Filter the list of dicts for values we want to keep and rename keys
    pulse_list = check_for_pulses(new_list) # Filter the list of dicts to remove actions, like renaming a board, that isn't really a project
    unique_pulse_ids = create_unique_pulse_id_list(pulse_list) # Make a list of the pulse id numbers
    gathered_projects = gather_dicts_by_pulse(unique_pulse_ids, pulse_list) # Organize the actions by project, so a list of lists of dicts
    filtered_projects = build_filtered_projects(gathered_projects)
    filtered_projects2 = define_project_types(filtered_projects)
    df = build_filtered_df(filtered_projects2)
    #df.to_excel("creative_projects.xlsx") ### UNCOMMENT IF YOU WANT TO GENERATE THE FILE ###
