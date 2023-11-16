import asyncio
import json
import random
import numpy as np
from tcdicn import Node
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import time

UNRESPONSIVE_THRESHOLD = 60  #(in seconds)
UNRESPONSIVE_CHECK_INTERVAL = 10 #(in seconds)

class DroneMonitorNode(Node):
    def _init_(self, drone_id, server, ttp=5, *args, **kwargs):
        super()._init_(*args, **kwargs)
        self.drone_id = drone_id
        self.server = server
        self.ttp = ttp  # Time To Propagate
        self.last_received_message_time = time.time()

    async def detect_unresponsive(self):
        while True:
            # Check if a drone has not sent a message within the specified time
            if (
                self.last_received_message_time is not None
                and time.time() - self.last_received_message_time > UNRESPONSIVE_THRESHOLD
            ):
                # Drone is unresponsive, notify controllers
                await self.server.set(f"status-{self.drone_id}", "Unresponsive")
            await asyncio.sleep(UNRESPONSIVE_CHECK_INTERVAL)

    async def monitor_drones(self):
        asyncio.create_task(self.detect_unresponsive())  # Start the unresponsive detection task

        history_buffer = []  # Buffer to store the last 100 readings
        while True:
            for drone_id in self.drones:
                # Subscribe to different types of sensor data for each drone separately
                position = await self.get(f"{drone_id}-position")
                temperature = await self.get(f"{drone_id}-temperature")
                battery = await self.get(f"{drone_id}-battery")

                # Process sensor data and update history buffer
                if all(x is not None for x in [position, temperature, battery]):
                    features = {
                        "temperature": float(temperature),
                        "battery": float(battery),
                        "position_0": float(position[0]),
                        "position_1": float(position[1]),
                    }
                    history_buffer.append(features)
                    if len(history_buffer) > 100:
                        history_buffer.pop(0)

            # Run ML model on the last 5 minutes of collected data
            recent_data = history_buffer[-300:]  # 300 seconds = 5 minutes
            if len(recent_data) > 0:
                features_matrix = [list(features.values()) for features in recent_data]
                failure_predictions = self.model.predict(features_matrix)

                # Publish alerts based on predictions
                for idx, failure_prediction in enumerate(failure_predictions):
                    if failure_prediction == 1:
                        await self.server.set(f"status-{self.drone_id}", "Failure-alert")

            # Wait a bit before the next monitoring cycle
            await asyncio.sleep(10)

    async def on_data(self, data, meta):
        # Update the last received message time when data is received
        self.last_received_message_time = time.time()
        await super().on_data(data, meta)

# Main Function
async def main():
    server = Node()
    await server.start(port=33333, dport=33334, ttl=180, tpf=60, client={"name": "inspector", "labels": ["failure-alert"], "ttp": 5})

    drones = ["drone06", "drone07", "drone08", "drone09", "drone10"]
    drone_nodes = [DroneMonitorNode(drone_id, server) for drone_id in drones]

    tasks = [node.monitor_drones() for node in drone_nodes]
    await asyncio.gather(*tasks)

if _name_ == "_main_":
    asyncio.run(main())
