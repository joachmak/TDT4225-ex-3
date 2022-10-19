""" OS (local) operations """
import os
import re
import datetime
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


# ACTIVITIES

def get_activities():
    """
    Get activity data for all users listed in labeled_ids.
    Format: (end_date, start_date, transportation_mode, user_id)
    """
    labeled_ids = get_labeled_ids()
    activity_data = []
    for user in labeled_ids:
        path = os.path.join(config("DATASET_ROOT_PATH"), "dataset", "Data", user, "labels.txt")
        # Format activity entries into a list
        activity_data += tuple(map(lambda line: tuple(line.split("\t") + [user]), read_rstrip_file(path)[1:]))
    return activity_data


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
    line.append(datetime.datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S"))
    return tuple(line)


def read_trackpoint_data(path, activities_dict: dict) -> list:
    """ Read trajectory data, ignore first 6 lines """
    with open(path) as f:
        lines = f.readlines()[6:]
        if len(lines) > constants.MAX_TRACKPOINT_COUNT:
            print(f"\t\t...Skipping file as it contains too many trackpoints (>{constants.MAX_TRACKPOINT_COUNT})")
            return []
        include_trackpoint = False
        start_time = None
        result = []
        temp_result = []
        for line in lines:
            l = _transform_trackpoint_line(line)
            if not include_trackpoint and str(l[3]) in activities_dict:
                print(f"\t\tFirst trackpoint for activity {str(l[3])}: {l}")
                include_trackpoint = True
                start_time = str(l[3])
            if include_trackpoint:
                temp_result.append(l + (activities_dict[start_time]["activity_id"],))
            if start_time is None:
                continue
            if str(l[3]) == str(activities_dict[start_time]["end_time"]):
                print(f"\t\tLast trackpoint for activity {str(l[3])}: {l}")
                include_trackpoint = False
                start_time = None
                result += temp_result
                temp_result.clear()
            if start_time is not None and l[3] > activities_dict[start_time]["end_time"]:
                include_trackpoint = False
                start_time = None
                temp_result.clear()
        return result


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