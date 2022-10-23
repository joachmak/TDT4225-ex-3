from datetime import datetime


class TrackPoint:
    def __init__(self, lat: float, lon: float, alt: int, time: datetime, activity_id: str, previous_time: str, uid: str):
        self.lat = lat
        self.lon = lon
        self.alt = alt
        self.time = time
        self.previous_time = previous_time  # time of previous trackpoint in this activity
        self.activity_id = activity_id
        self.uid = uid

    def __str__(self):
        return f"(pos: ({self.lat}, {self.lon}), alt: {self.alt}, activity {self.activity_id}, " \
               f"{self.time})"
