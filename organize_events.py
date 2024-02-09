# Python built-in modules
from collections import OrderedDict
from collections.abc import MutableMapping
import datetime
import json

# Imported modules
import pandas as pd

"""
Work toward taking a json file of all activities related to an event posting, clean that up and gather
these by event project (pulse).
"""

# Opening JSON file
data_file = '/Users/douglasray/Projects/monday_actions/data_files/events/2021-23_events_merged.json'
f = open(data_file)
data = json.load(f)

def build_pulse_events_list(data):
    """
    Use the json data to create list of dicts for each event action
    and clean this up a bit to select only a few fields and put them
    on the same level of a given event dict. This also selects only those
    for 'pulses', which are event postings rather actions like renaming boards.
    """
    
    # Create a list of dicts for each action if it involves a 'pulse'
    # with the dicts using only certain key:value pairs from the data.
    pulse_dict_list = [] # This becomes the list of event action dicts
    for item in data:
        action_dict = {}
        if 'pulse' in item['entity']:
            action_dict['action_id'] = item['id']
            action_dict['event'] = item['event']
            action_dict['action_date'] = int(item['created_at'])/10000000 # Several steps to convert unix string to datatime obj
            action_dict['action_date'] = datetime.datetime.fromtimestamp(action_dict['action_date']).date() # Convert to datetime date
            data_level = json.loads(item['data'])
            action_dict['data'] = data_level
            if 'group_id' in data_level.keys():
                action_dict['unit_id'] = data_level['group_id']
            if 'group_name' in data_level.keys():
                action_dict['unit_name'] = data_level['group_name']
            if 'pulse_name' in data_level.keys():
                action_dict['pulse_name'] = data_level['pulse_name']
            if 'create_pulse' in action_dict.values():
                action_dict['created_date'] = action_dict['action_date']
            if 'archive_pulse' in action_dict.values():
                action_dict['archive_date'] = action_dict['action_date']
            action_dict.pop('data')
            if 'pulse_id' in data_level.keys():
                action_dict['pulse_id'] = data_level['pulse_id']
                pulse_dict_list.append(action_dict) # Only add the ones with a pulse ID
            else:
                pass

    # Gather events where the action was to 'create_pulse' into a list of dictionaries
    created_events = [] # This becomes a list of created pulses
    for item in pulse_dict_list:
        if 'create_pulse' in item.values():
            created_events.append(item)

    # Gather events where the action was to 'archive_pulse' into a list of dictionaries.
    archived_events = [] # This becomes a list of archived pulses
    for item in pulse_dict_list:
        if 'archive_pulse' in item.values():
            archived_events.append(item)

    # Create a list of pulse_ids for archived events
    archived_event_ids = []
    for item in archived_events:
        archived_event_ids.append(item['pulse_id'])
        archived_event_ids = list(set(archived_event_ids)) # remove duplicates

    return created_events, archived_events, archived_event_ids

"""
Separating created events from archived events lists because while events are created once,
they often are archived more than once. So first we'll filter list of archived events to keep
only the last one for each pulse, then merge created with archived actions per pulse into dicts
for each pulse.
"""

created_events, archived_events, archived_event_ids = build_pulse_events_list(data)

def consolidate_pulses(archived_events, pulse_id):
    """
    Make a list of all archive actions for given pulse_id
    but then just return dict with the last date archived
    """
    pulse_dicts = [] # A list for each pulse_id
    for item in archived_events: 
        if item['pulse_id'] == pulse_id:
            pulse_dicts.append(item)
    last_archive = max(pulse_dicts, key=lambda x: x['archive_date']) # Sort for latest date
    return last_archive

# Gather dicts each archived pulse into a list
consolidated_archived_events = []
for pulse_id in archived_event_ids:
    last_archive = consolidate_pulses(archived_events, pulse_id)
    consolidated_archived_events.append(last_archive)

# Next: merge the created and archived dicts by pulse_id into a single dict per pulse
def merge_created_with_archived(created_events, consolidated_archived_events, pulse_id):
    new_dict = {}
    for item in created_events:
        if item['pulse_id'] == pulse_id:
            new_dict['pulse_id'] = pulse_id
            new_dict['pulse_name'] = item['pulse_name']
            new_dict['unit_id'] = item['unit_id']
            if 'unit_name' in item.keys():
                new_dict['unit_name'] = item['unit_name']
            new_dict['created_date'] = item['created_date']
    for elem in consolidated_archived_events:
        if elem['pulse_id'] == pulse_id:
            if 'archive_date' in elem.keys():
                new_dict['archive_date'] = elem['archive_date']
    return new_dict

if __name__ == "__main__":
    # This becomes our finished data set.
    consolidated_event_dicts = []
    for pulse_id in archived_event_ids:
        new_dict = merge_created_with_archived(created_events, consolidated_archived_events, pulse_id)
        consolidated_event_dicts.append(new_dict)
    df = pd.DataFrame(consolidated_event_dicts)
    df.to_excel('event_actions.xlsx')
