import utils
import pandas as pd
import datetime
import numpy as np
from code_cs8803.utils import bag_to_svmlight

def read_csv(filepath):
    
    '''
    TODO: This function needs to be completed.
    Read the events.csv, mortality_events.csv and event_feature_map.csv files into events, mortality and feature_map.
    
    Return events, mortality and feature_map
    '''

    #Columns in events.csv - patient_id,event_id,event_description,timestamp,value
    events = pd.read_csv(filepath + 'events.csv')
    
    #Columns in mortality_event.csv - patient_id,timestamp,label
    mortality = pd.read_csv(filepath + 'mortality_events.csv')

    #Columns in event_feature_map.csv - idx,event_id
    feature_map = pd.read_csv(filepath + 'event_feature_map.csv')

    return events, mortality, feature_map


def calculate_index_date(events, mortality, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Create list of patients alive ( mortality_events.csv only contains information about patients deceased)
    2. Split events into two groups based on whether the patient is alive or deceased
    3. Calculate index date for each patient
    
    IMPORTANT:
    Save indx_date to a csv file in the deliverables folder named as etl_index_dates.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, indx_date.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=False)

    Return indx_date
    '''

    dead_ids = mortality.patient_id.unique()

    #First calculate index_date of deceased patients
    deceased_index_date = mortality.groupby('patient_id').timestamp.\
        agg(lambda x: datetime.datetime.strptime(x.max(), "%Y-%m-%d") - datetime.timedelta(days=30))

    alive_index_date = events[events['patient_id'].isin(dead_ids) == False].groupby('patient_id').timestamp.\
        agg(lambda x: datetime.datetime.strptime(x.max(), "%Y-%m-%d"))

    indx_date = pd.concat([deceased_index_date, alive_index_date], axis=0)
    indx_date.name = 'indx_date'
    indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', index=True, header=True)
    return indx_date


def filter_events(events, indx_date, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Join indx_date with events on patient_id
    2. Filter events occuring in the observation window(IndexDate-2000 to IndexDate)
    
    
    IMPORTANT:
    Save filtered_events to a csv file in the deliverables folder named as etl_filtered_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)

    Return filtered_events
    '''
    obs_window = 2000

    events['timestamp'] = events['timestamp'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))

    filtered_events = pd.DataFrame()
    for i in indx_date.index:
        mask = (events['patient_id'] == i) & (events['timestamp'] <= indx_date[i]) & (events['timestamp'] >= (indx_date[i] - datetime.timedelta(days=obs_window)))
        filtered_events = filtered_events.append(events.loc[mask])

    filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)
    return filtered_events


def aggregate_events(filtered_events_df, mortality_df, feature_map_df, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Replace event_id's with index available in event_feature_map.csv
    2. Remove events with n/a values
    3. Aggregate events using sum and mean to calculate feature value 
    4. Normalize the values obtained above using min-max normalization
    
    
    IMPORTANT:
    Save aggregated_events to a csv file in the deliverables folder named as etl_aggregated_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header .
    For example if you are using Pandas, you could write: 
        aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    Return filtered_events
    '''
    filtered_events_df = filtered_events_df.dropna()

    diag_events_df = filtered_events_df[filtered_events_df['event_id'].str.contains("DIAG|DRUG")]
    lab_events_df = filtered_events_df[filtered_events_df['event_id'].str.contains("LAB")]

    diag_events_df = diag_events_df.groupby(['patient_id', 'event_id'],as_index=False)['value'].sum()
    lab_events_df = lab_events_df.groupby(['patient_id', 'event_id'], as_index=False)['value'].count()

    diag_events_df['event_id'] = diag_events_df['event_id'].\
        apply(lambda x: feature_map_df.loc[feature_map_df.event_id == x, 'idx'].values[0])
    diag_events_df = diag_events_df.rename(columns={'event_id': 'feature_id', 'value': 'feature_value'})

    lab_events_df['event_id'] = lab_events_df['event_id'].\
        apply(lambda x: feature_map_df.loc[feature_map_df.event_id == x, 'idx'].values[0])
    lab_events_df = lab_events_df.rename(columns={'event_id': 'feature_id', 'value': 'feature_value'})

    combined_df = [diag_events_df, lab_events_df]
    combined_df = pd.concat(combined_df)

    combined_df['feature_value'] = combined_df.groupby('feature_id', as_index=False)['feature_value'].transform(lambda x: x/x.max())

    aggregated_events = combined_df

    aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    return aggregated_events

def create_features(events, mortality, feature_map):
    
    deliverables_path = '../deliverables/'

    #Calculate index date
    indx_date = calculate_index_date(events, mortality, deliverables_path)

    #Filter events in the observation window
    filtered_events = filter_events(events, indx_date, deliverables_path)
    
    #Aggregate the event values for each patient 
    aggregated_events = aggregate_events(filtered_events, mortality, feature_map, deliverables_path)

    '''
    TODO: Complete the code below by creating two dictionaries - 
    1. patient_features :  Key - patient_id and value is array of tuples(feature_id, feature_value)
    2. mortality : Key - patient_id and value is mortality label
    '''
    patient_features_dict = {}
    for index, row in aggregated_events.iterrows():
        if row['patient_id'] not in patient_features_dict.keys():
            patient_features_dict[row['patient_id']] = []

        patient_features_dict[row['patient_id']].append((row['feature_id'], row['feature_value']))

    mortality_list = {}
    for index, row in mortality.iterrows():
        mortality_list[row['patient_id']] = row['label']

    return patient_features_dict, mortality_list

def save_svmlight(patient_features, mortality, op_file, op_deliverable):
    
    '''
    TODO: This function needs to be completed
    Create two files:
    1. op_file - which saves the features in svmlight format. (See instructions in Q3d for detailed explanation)
    2. op_deliverable - which saves the features in following format:
       patient_id1 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...
       patient_id2 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...  
    
    Note: Please make sure the features are ordered in ascending order, and patients are stored in ascending order as well.     
    '''
    deliverable1 = open(op_file, 'w')
    deliverable2 = open(op_deliverable, 'w')
    
    deliverable1.write("")

    for key, items in patient_features.items():
        op_text = str(int(mortality[key])) + " " + bag_to_svmlight(sorted(items))
        deliverable1.write(op_text)

    for key, items in patient_features.items():
        op_text = str(int(key)) + " " + str(mortality[key]) + " " + bag_to_svmlight(sorted(items))
        deliverable2.write(op_text)

    deliverable1.close()
    deliverable2.close()


def main():
    train_path = '../data/train/'
    events, mortality, feature_map = read_csv(train_path)
    patient_features, mortality = create_features(events, mortality, feature_map)
    save_svmlight(patient_features, mortality, '../deliverables/features_svmlight.train', '../deliverables/features.train')

if __name__ == "__main__":
    main()




