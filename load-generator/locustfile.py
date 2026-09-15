import random
import uuid
from locust import HttpUser, task, between


class Player(HttpUser):
    wait_time = between(0.5, 2)

    def on_start(self):
        self.player_id = f"player-{uuid.uuid4().hex[:8]}"
        self.client.post("/events", json={
            "player_id": self.player_id,
            "event_type": "connect"
        })

    @task
    def send_action(self):
        self.client.post("/events", json={
            "player_id": self.player_id,
            "event_type": "action"
        })

    def on_stop(self):
        self.client.post("/events", json={
            "player_id": self.player_id,
            "event_type": "disconnect"
        })
