from datetime import datetime


class Activity:
    def __init__(self, _id: str, transportation_mode: str, start_time: datetime, end_time: datetime, uid: str):
        self._id = _id
        self.transportation_mode = transportation_mode
        self.start_time = start_time
        self.end_time = end_time
        self.uid = uid  # We can have two-way references, as we will write the data once and never update it
        # Therefore, maintaining consistency in the data won't be a challenge
        # We cannot two-way-reference track points, because it could exceed our 16MB doc limit

    def __str__(self):
        return f"({self.transportation_mode}, {self.start_time} - {self.end_time}, user {self.uid})"
