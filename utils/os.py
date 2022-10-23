""" OS (local) operations """
import logging
import os
import re
import uuid
import classes
import utils.constants as constants
from utils.db import insert_class_data
from utils.db_connector import DbConnector
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


def upload_activities_and_trackpoints(connection: DbConnector) -> dict:
    """
    Get activity data for all users.
    Each .plt file is an activity. If the .plt file doesn't exactly match with a labeled activity on start- and end-date,
    then the trackpoints are added to an unlabeled activity. Non-matching .plt files are added to an activity that has
    start_date and end_date that matches the .plt file's first and last row.
    Thus, all activites should have trackpoints that exactly match their start- and end-date.
    This method calls db operations, which is not preferable, but slightly improves efficiency because the "setup"
    doesn't have to happen before each insert.
    """
    logger = logging.getLogger("getActivities")
    user_directories = get_all_users()
    user_directories.sort(key=lambda x: int(x))
    labeled_users = get_labeled_users()
    total_trackpoints_inserted = 0
    total_activities_inserted = 0
    user_activities: dict = {}  # (user_id: str -> activity_ids: str[] )
    # for each user
    for user_dir in user_directories:  # user_dir is a str with the user id as a name
        activities_to_insert: list = []
        trackpoints_to_insert: list = []
        print("Scanning user " + user_dir)
        base_path = os.path.join(config("DATASET_ROOT_PATH"), "dataset", "Data", user_dir)
        trajectory_path = os.path.join(base_path, "Trajectory")
        possible_labels_path = os.path.join(base_path, "labels.txt")
        labeled_activities_dict = {}
        skipped_plt_files_count = 0
        file_count = 0
        if user_dir not in user_activities:  # initialize arr for storing activity ids of user
            user_activities[user_dir] = []
        # generate labeled activities dict: (key, val) = (start_date, labeled_activity_data)
        if user_dir in labeled_users:
            print("\tTHIS IS A LABELED USER")
            labels = get_labels_from_file(possible_labels_path)
            for transportation_mode in labels:
                start = transportation_mode[0]
                labeled_activities_dict[start] = transportation_mode

        # go through each plt file (1 .plt file is 1 activity)
        for _, __, files in os.walk(trajectory_path, topdown=False):
            file_count = len(files)
            # print(f"{len(files)} FILES IN TOTAL\n\n")
            print(f"\tReading {len(files)} .plt-files...")
            for plt_filename in files:
                activity_id = uuid.uuid4()  # generate activity id
                data = read_trackpoint_data(os.path.join(trajectory_path, plt_filename), user_dir)
                if len(data) > 0:  # If there are <=2500 trackpoints
                    first_tp = data[0]
                    last_tp = data[len(data) - 1]
                    activity_start_date = first_tp[3]
                    activity_end_date = last_tp[3]
                    transportation_mode = None
                    if activity_start_date in labeled_activities_dict and activity_end_date in labeled_activities_dict[activity_start_date]:
                        # Add labeled activity
                        transportation_mode = labeled_activities_dict[activity_start_date][2]
                        print("\t\t.plt file " + plt_filename + " is a labeled activity with transportation Label " + transportation_mode)
                    activity: classes.Activity = classes.Activity(str(activity_id), transportation_mode, activity_start_date, activity_end_date, user_dir)
                    activities_to_insert.append(activity)
                    user_activities[user_dir].append(str(activity_id))
                    trackpoints_to_insert += process_trackpoint_data(data, activity_id)
                else:
                    skipped_plt_files_count += 1
        total_activities_inserted += len(activities_to_insert)
        total_trackpoints_inserted += len(trackpoints_to_insert)
        insert_class_data(connection, constants.COLLECTION_ACTIVITIES, activities_to_insert,
                          f"\tInserting {len(activities_to_insert)} activities for user {user_dir} ...")
        insert_class_data(connection, constants.COLLECTION_TRACKPOINTS, trackpoints_to_insert,
                          f"\tInserting {len(trackpoints_to_insert)} trackpoints for user {user_dir} ...")
        print(f"\tTotal: {total_trackpoints_inserted} trackpoints, {total_activities_inserted} activities")
        # Logging to file (see 'log'-directory)
        if skipped_plt_files_count + len(activities_to_insert) == file_count:
            logger.debug(f"\tSkipped {skipped_plt_files_count} .plt files (activities) because they had >2500 "
                         f"trackpoints. {skipped_plt_files_count} + {len(activities_to_insert)} = {file_count}")
        else:
            logger.error(f"\tSkipped {skipped_plt_files_count} .plt files (activities) because they had >2500 "
                         f"trackpoints AND THIS IS INCORRECT!!!")
    return user_activities


def process_trackpoint_data(trackpoint_data, activity_id):
    return list(map(lambda tp: classes.TrackPoint(tp[0], tp[1], tp[2], tp[3], str(activity_id), tp[4], tp[5]), trackpoint_data))


def _is_float(string: str):
    return re.match(r'^-?\d+(?:\.\d+)$', string)


def _transform_trackpoint_line(line: str, previous_time: str or None, uid: str):
    """
    Convert trackpoint file line to tuple, convert float values to float, combine date and time cols
    Format: (lat, lon, alt, time, previous_time)
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
    line.append(previous_time)
    line.append(uid)
    return tuple(line)


def read_trackpoint_data(path, uid: str) -> list:
    """ Read trajectory data, ignore first 6 lines. Return empty list if there are >2500 trackpoints. """
    with open(path) as f:
        lines = f.readlines()[6:]
        if len(lines) > constants.MAX_TRACKPOINT_COUNT:
            return []
        previous_time = None
        res = []
        for i in range(len(lines)):
            line = _transform_trackpoint_line(lines[i], previous_time, uid)
            res.append(line)
            previous_time = line[3]
        return res

