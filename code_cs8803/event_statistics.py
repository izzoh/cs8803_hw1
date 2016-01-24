import time
import pandas as pd
import numpy as np


def read_csv(filepath):
    '''
    Read the events.csv and mortality_events.csv files. Variables returned from this function are passed as input to the metric functions.
    This function needs to be completed.
    '''
    events = pd.read_csv(filepath + 'events.csv')

    mortality = pd.read_csv(filepath + 'mortality_events.csv')

    return events, mortality


def event_count_metrics(events, mortality):
    '''
    Event count is defined as the number of events recorded for a given patient.
    This function needs to be completed.
    '''

    dead_ids = mortality.patient_id.unique()

    avg_dead_event_count = events[events['patient_id'].isin(dead_ids)].patient_id.value_counts().mean()

    max_dead_event_count = events[events['patient_id'].isin(dead_ids)].patient_id.value_counts().max()

    min_dead_event_count = events[events['patient_id'].isin(dead_ids)].patient_id.value_counts().min()

    avg_alive_event_count = events[events['patient_id'].isin(dead_ids) == False].patient_id.value_counts().mean()

    max_alive_event_count = events[events['patient_id'].isin(dead_ids) == False].patient_id.value_counts().max()

    min_alive_event_count = events[events['patient_id'].isin(dead_ids) == False].patient_id.value_counts().min()

    return min_dead_event_count, max_dead_event_count, avg_dead_event_count, min_alive_event_count, max_alive_event_count, avg_alive_event_count


def encounter_count_metrics(events, mortality):
    '''
    Encounter count is defined as the count of unique dates on which a given patient visited the ICU. 
    This function needs to be completed.
    '''
    dead_ids = mortality.patient_id.unique()

    avg_dead_encounter_count = events[events['patient_id'].isin(dead_ids)].groupby(['patient_id'])['timestamp']. \
        agg(lambda x: np.size(np.unique(x.values))).mean()

    max_dead_encounter_count = events[events['patient_id'].isin(dead_ids)].groupby(['patient_id'])['timestamp']. \
        agg(lambda x: np.size(np.unique(x.values))).max()

    min_dead_encounter_count = events[events['patient_id'].isin(dead_ids)].groupby(['patient_id'])['timestamp']. \
        agg(lambda x: np.size(np.unique(x.values))).min()

    avg_alive_encounter_count = events[events['patient_id'].isin(dead_ids) == False].groupby(['patient_id'])[
        'timestamp']. \
        agg(lambda x: np.size(np.unique(x.values))).mean()

    max_alive_encounter_count = events[events['patient_id'].isin(dead_ids) == False].groupby(['patient_id'])[
        'timestamp']. \
        agg(lambda x: np.size(np.unique(x.values))).max()

    min_alive_encounter_count = events[events['patient_id'].isin(dead_ids) == False].groupby(['patient_id'])[
        'timestamp']. \
        agg(lambda x: np.size(np.unique(x.values))).min()

    return min_dead_encounter_count, max_dead_encounter_count, avg_dead_encounter_count, min_alive_encounter_count, max_alive_encounter_count, avg_alive_encounter_count


def record_length_metrics(events, mortality):
    '''
    Record length is the duration between the first event and the last event for a given patient. 
    This function needs to be completed.
    '''
    dead_ids = mortality.patient_id.unique()

    avg_dead_rec_len = events[events['patient_id'].isin(dead_ids)].groupby(['patient_id']).timestamp. \
        agg(lambda x: (diff_record_time(x.max(), x.min()))).mean()

    max_dead_rec_len = events[events['patient_id'].isin(dead_ids)].groupby(['patient_id']).timestamp. \
        agg(lambda x: (diff_record_time(x.max(), x.min()))).max()

    min_dead_rec_len = events[events['patient_id'].isin(dead_ids)].groupby(['patient_id']).timestamp. \
        agg(lambda x: (diff_record_time(x.max(), x.min()))).min()

    avg_alive_rec_len = events[events['patient_id'].isin(dead_ids) == False].groupby(['patient_id']).timestamp. \
        agg(lambda x: (diff_record_time(x.max(), x.min()))).mean()

    max_alive_rec_len = events[events['patient_id'].isin(dead_ids) == False].groupby(['patient_id']).timestamp. \
        agg(lambda x: (diff_record_time(x.max(), x.min()))).max()

    min_alive_rec_len = events[events['patient_id'].isin(dead_ids) == False].groupby(['patient_id']).timestamp. \
        agg(lambda x: (diff_record_time(x.max(), x.min()))).min()

    return min_dead_rec_len, max_dead_rec_len, avg_dead_rec_len, min_alive_rec_len, max_alive_rec_len, avg_alive_rec_len


def diff_record_time(start_time, end_time):
    """ This method will diff two time_struct objects and produce a result that is the days between the two events

    :param start_time:
    :type start_time:
    :param end_time:
    :type end_time:
    :return:
    :rtype:
    """
    diff = (time.mktime(time.strptime(start_time, "%Y-%m-%d")) -
            time.mktime(time.strptime(end_time, "%Y-%m-%d"))) / 86400

    return diff

def main():
    '''
    DONOT MODIFY THIS FUNCTION. 
    Just update the train_path variable to point to your train data directory.
    '''
    # Modify the filepath to point to the CSV files in train_data
    train_path = ''
    events, mortality = read_csv(train_path)

    # Compute the event count metrics
    start_time = time.time()
    event_count = event_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute event count metrics: " + str(end_time - start_time) + "s")
    print event_count

    # Compute the encounter count metrics
    start_time = time.time()
    encounter_count = encounter_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute encounter count metrics: " + str(end_time - start_time) + "s")
    print encounter_count

    # Compute record length metrics
    start_time = time.time()
    record_length = record_length_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute record length metrics: " + str(end_time - start_time) + "s")
    print record_length


if __name__ == "__main__":
    main()
