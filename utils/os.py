""" OS (local) operations """
import os
import re
import uuid

import classes
import utils.constants as constants
from decouple import config


def read_rstrip_file(path) -> list:
    """ Right-strip lines and read file content """
    with open(path) as f:
        return list(map(lambda line: line.rstrip("\n"), f.readlines()))


def get_labeled_ids() -> list:
    """ Returns labeled ids as a list, don't control list with directory structure """
    return read_rstrip_file(os.path.join(config("DATASET_ROOT_PATH"), "dataset", "labeled_ids.txt"))


def get_all_users() -> list:
    """ Get all users based on directory names """
    result = []
    for root, dirs, files in os.walk(os.path.join(config("DATASET_ROOT_PATH"), "dataset", "Data"), topdown=False,
                                     followlinks=False):
        for dir_name in dirs:
            if dir_name.isnumeric():
                result.append(dir_name)
    return result


def get_labeled_users() -> list:
    """ Return list of user IDs with labeled activities, based on directory structure """
    labeled_ids = get_labeled_ids()
    all_users = get_all_users()
    # get all label ids that have an associated directory
    return list(filter(lambda labeled_id: labeled_id in all_users, labeled_ids))


def get_labels_from_file(filepath):
    lines = read_rstrip_file(filepath)
    res = []
    for line in lines[1:]:
        res.append(line.split("\t"))
    return res


# ACTIVITIES
def get_activities():
    """
    Get activity data for all users listed in labeled_ids.
    Format: (end_date, start_date, transportation_mode, user_id)
    """
    activities_to_insert = []
    user_directories = get_all_users()
    user_directories.sort(key=lambda x: int(x))
    labeled_users = get_labeled_users()

    for dir in user_directories:
        print("Scanning user " + dir)
        base_path = os.path.join(config("DATASET_ROOT_PATH"), "dataset", "Data", dir)
        trajectory_path = os.path.join(base_path, "Trajectory")
        possible_labels_path = os.path.join(base_path, "labels.txt")
        labeled_activities_dict = {}
        if dir in labeled_users:
            print("THIS IS A LABELED USER")
            labels = get_labels_from_file(possible_labels_path)
            #print(labels)
            for label in labels:
                start = label[0]
                labeled_activities_dict[start] = label


        #print(".plt files for user " + dir + ":")
        for _, __, files in os.walk(trajectory_path, topdown=False):
            #print(f"{len(files)} FILES IN TOTAL\n\n")
            for plt_filename in files:
                _id = uuid.uuid4()
                #print("FILE " + plt_filename)
                data = read_trackpoint_data(os.path.join(trajectory_path, plt_filename))
                if len(data) > 0:
                    first_tp = data[0]
                    last_tp = data[len(data) - 1]
                    start_date = first_tp[3]
                    end_date = last_tp[3]
                    label = None
                    if start_date in labeled_activities_dict and end_date in labeled_activities_dict[start_date]:
                        # Add labeled activity
                        label = labeled_activities_dict[start_date][2]
                        print("PLT FILE " + plt_filename + " IS A LABELED ACTIVITY !!! Label " + label)
                    activities_to_insert.append(classes.Activity(str(_id), label, start_date, end_date, dir))
                    add_trackpoints(data, _id)
                    # Add unlabeled activity



def add_trackpoints(trackpoint_data, activity_id):
    print("\n\n\n")
    tp_to_insert = list(map(lambda tp: (*tp, activity_id), trackpoint_data))
    for tp in tp_to_insert[0:10]:
        print(tp)

# TRACK POINTS

def _is_float(string: str):
    return re.match(r'^-?\d+(?:\.\d+)$', string)


def _transform_trackpoint_line(line: str):
    """
    Convert trackpoint file line to tuple, convert float values to float, combine date and time cols
    Format: (lat, lon, alt, datetime)
    """
    line = list(map(
        lambda elem: float(elem) if _is_float(elem)
        else int(elem) if elem.isnumeric()
        else elem,
        line.rstrip("\n").split(",")))
    line.pop(2)  # Always 0
    line.pop(3)  # We use datetime instead
    # merge date and time
    date = line.pop(3)
    time = line.pop(3)
    line.append(f"{date.replace('-', '/')} {time}")
    return tuple(line)


def read_trackpoint_data(path) -> list:
    """ Read trajectory data, ignore first 6 lines """
    with open(path) as f:
        lines = f.readlines()[6:]
        if len(lines) > constants.MAX_TRACKPOINT_COUNT:
            #print(f"\t\t...Skipping file as it contains too many trackpoints (>{constants.MAX_TRACKPOINT_COUNT})")
            return []
        return list(map(lambda line: _transform_trackpoint_line(line), lines))


def get_trackpoints(user_id: str, activities_dict: dict) -> list:
    """
    Get list of trackpoints for user, given a list of activities sorted on start_date
    Format: (lat, lon, alt, datetime)
    """
    print(f"Getting trackpoints for user {user_id}")
    base_path = os.path.join(config("DATASET_ROOT_PATH"), "dataset", "Data", user_id, "Trajectory")
    data = []
    counter = 1
    for _, __, files in os.walk(base_path, topdown=False):
        for filename in files:
            full_path = os.path.join(base_path, filename)
            print(f"\t...Reading trackpoints from file {counter} at {full_path}")
            data += read_trackpoint_data(full_path, activities_dict)
            counter += 1
    return data
