
class User:
    def __init__(self, _id: str, has_labels: bool, activity_ids: list):
        self._id = _id
        self.has_labels = has_labels
        self.activity_ids = activity_ids

    def __str__(self):
        return f"(uid {self._id})"
