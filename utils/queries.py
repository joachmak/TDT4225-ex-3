""" Query strings """
GET_USER_COUNT = """db.User.aggregate([{$count: "Users count"}])"""
GET_ACTIVITY_COUNT = """db.Activity.aggregate([{$count: "Activities count"}])"""
GET_TRACKPOINT_COUNT = """db.Trackpoint.aggregate([{$count: "Trackpoints count"}])"""

