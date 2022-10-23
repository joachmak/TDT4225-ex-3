from utils.constants import COLLECTION_ACTIVITIES, COLLECTION_USERS
from utils.db_connector import DbConnector
import pprint


def task_2(connection: DbConnector):
    """Find average activity count per user"""
    users = connection.db[COLLECTION_USERS].find({})
    user_list = list(map(lambda user: [user["_id"], len(user["activity_ids"])], users))
    activity_count = 0
    for record in user_list:
        activity_count += record[1]
    print(f"Average activity count per user: {round(activity_count/len(user_list),2)}")


def task_5(connection: DbConnector):
    """ Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels """
    activities = connection.db[COLLECTION_ACTIVITIES].find({})  # get all activities
    t_mode_dict = {}
    for activity in activities:
        transportation_mode = activity["transportation_mode"]
        if transportation_mode is None:
            continue
        if transportation_mode not in t_mode_dict:
            t_mode_dict[transportation_mode] = 0
        t_mode_dict[transportation_mode] += 1
    pprint.pprint(t_mode_dict)


def task_11(connection: DbConnector):
    """Find users with transportation modes and their most used transportation mode"""
    print("Users and their most used transportation mode")
    activities = connection.db[COLLECTION_ACTIVITIES].find({})
    dict = {}
    for element in activities:
        activity = str(element)
        array_activity = activity.split(",")
        transport = array_activity[1]
        if transport!=" 'transportation_mode': None":
            transportation_mode = transport.split(":")[1].replace("'","")
            user_id = array_activity[4].split(":")[1].replace("}","")
            #creates dictionary with user_id as key and all transportation_modes as values
            if user_id in dict:
                dict[user_id].append(transportation_mode)
            else:
                dict[user_id] = [transportation_mode]
    #finds value with the most occurrences for each user
    for key in dict:
        dict[key] = max(set(dict[key]), key=dict[key].count)
    pprint.pprint(dict)




def main():
    conn: DbConnector = DbConnector()
    #task_2(conn)
    task_5(conn)
    #task_11(conn)
    conn.close_connection()


if __name__ == "__main__":
    main()
