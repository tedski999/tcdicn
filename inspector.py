import asyncio
import json
import random
import numpy as np
from tcdicn import Node
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

class DroneMonitorNode(Node):
    def __init__(self, drone_id, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.drone_id = drone_id
        self.server = server
        self.model = DecisionTreeClassifier(random_state=42)
        self.feature_columns = ["temperature", "battery", "position_0", "position_1"]
        self.target_column = "failure"

    async def train_ml_model(self):
        # Train the ML model using historical data
        historical_data = await self._collect_historical_data()
        X_train, X_test, y_train, y_test = train_test_split(
            historical_data[self.feature_columns], historical_data[self.target_column], test_size=0.2, random_state=42
        )
        self.model.fit(X_train, y_train)
        predictions = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        print(f"ML Model trained with accuracy: {accuracy}")

    async def monitor_drone(self):
        await self.train_ml_model()  # Train the ML model initially
        while True:
            # Subscribe to sensor data for the drone
            position = await self.get(f"{self.drone_id}-position")
            temperature = await self.get(f"{self.drone_id}-temperature")
            battery = await self.get(f"{self.drone_id}-battery")

            # Process sensor data
            if all(x is not None for x in [position, temperature, battery]):
                # Prepare features for prediction
                features = {
                    "temperature": float(temperature),
                    "battery": float(battery),
                    "position_0": float(position[0]),
                    "position_1": float(position[1]),
                }

                # Make prediction
                failure_prediction = self.model.predict([list(features.values())])[0]

                # Publish alert based on prediction
                if failure_prediction == 1:
                    await self.server.set(f"status-{self.drone_id}", "Failure-alert")

            # Wait a bit before the next monitoring cycle
            await asyncio.sleep(10)

    async def _collect_historical_data(self):
        historical_data = {
            "temperature": [],
            "battery": [],
            "position_0": [],
            "position_1": [],
            "failure": [],
        }

        # Simulate data for 100 time steps
        for _ in range(100):
            # Simulate normal operating conditions
            temperature = np.random.normal(loc=25, scale=5)
            battery = np.random.uniform(low=20, high=100)
            position_0 = np.random.uniform(low=0, high=10)
            position_1 = np.random.uniform(low=0, high=10)
            failure = 0  # No failure

            # Introduce failure scenarios randomly
            if random.random() < 0.1:  # 10% chance of failure
                failure = 1  # Failure

            # Append data to historical_data
            historical_data["temperature"].append(temperature)
            historical_data["battery"].append(battery)
            historical_data["position_0"].append(position_0)
            historical_data["position_1"].append(position_1)
            historical_data["failure"].append(failure)

        return historical_data

async def main():
    server = Node()
    await server.start(port=33333, dport=33334, ttl=180, tpf=60, client={"name": "inspector", "labels": ["failure-alert"]})

    drones = ["drone06", "drone07", "drone08", "drone09", "drone10"]
    drone_nodes = [DroneMonitorNode(drone_id, server) for drone_id in drones]

    tasks = [node.monitor_drone() for node in drone_nodes]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
