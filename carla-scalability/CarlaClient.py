"""Script containing an abstraction class for creating the CARLA environment."""
import json
import random
import math
from typing import Optional
from datetime import datetime
import carla

class CarlaClient:
    """
    Class for creating the CARLA environment. Specify the camera resolution, fps
    and the simulation length.
    """
    def __init__(self, host: str, port: int, img_width: int, img_height: int,
                 fps: int, lidar_points_per_second: int):
        self.vehicle_list = []
        self.camera_sensor_list = []
        self.lidar_sensor_list = []
        # self.lidar_list = []
        self.ai_controller_list = []
        self.pedestrian_list = []
        self.intersections = []
        self.img_width = img_width
        self.img_height = img_height
        self.lidar_points_per_second = lidar_points_per_second
        self.fps = fps
        self.client = carla.Client(host, port)
        self.world = self.client.get_world()
        self.original_settings = self.world.get_settings()
        self.traffic_manager = self.client.get_trafficmanager()
        self.blueprint_library = self.world.get_blueprint_library()
        self.map = self.world.get_map()
        self.spawn_points = self.map.get_spawn_points()
        self.camera_bp = self.blueprint_library.find('sensor.camera.rgb')
        self.camera_bp.set_attribute('image_size_x', f'{self.img_width}')
        self.camera_bp.set_attribute('image_size_y', f'{self.img_height}')
        self.lidar_bp = self.blueprint_library.find('sensor.lidar.ray_cast')
        self.lidar_bp.set_attribute('points_per_second', f'{lidar_points_per_second}')

    def set_sync_mode(self) -> None:
        """
        Run the CARLA simulation in synchronous mode.
        Also sets the traffic manager to synchronous mode.
        """
        settings = self.world.get_settings()
        settings.synchronous_mode = True
        settings.fixed_delta_seconds = 1 / self.fps
        self.world.apply_settings(settings)
        self.traffic_manager.set_synchronous_mode(True)

    def disable_sync_mode(self) -> None:
        """
        Run the CARLA simulation in synchronous mode.
        Also sets the traffic manager to synchronous mode.
        """
        settings = self.world.get_settings()
        settings.synchronous_mode = False
        self.world.apply_settings(settings)
        self.traffic_manager.set_synchronous_mode(False)

    def generate_spawn_points(self, n_points: int) -> list:
        """Generate random spawn points for the pedestrians."""
        spawn_points = []
        for _ in range(n_points):
            spawn_points.append(self.world.get_random_location_from_navigation())
        return spawn_points

    def spawn_pedestrians_to_points(self, spawn_point_indices: list) -> None:
        """
        Spawn pedestrians to predefined spawn points. The spawn points can be visualized
        using `visualize_spawn_points.py`.
        """
        walker_controller_bp = self.blueprint_library.find('controller.ai.walker')
        spawn_points = self.generate_spawn_points(300)
        for i in spawn_point_indices:
            walker_bp = random.choice(self.blueprint_library.filter('walker.*'))
            transform = carla.Transform(spawn_points[i])
            pedestrian = self.world.try_spawn_actor(walker_bp, transform)
            if pedestrian is None:
                continue
            self.pedestrian_list.append(pedestrian)
            ai_controller = self.world.spawn_actor(walker_controller_bp, carla.Transform(),
                                                   attach_to=pedestrian)
            self.ai_controller_list.append(ai_controller)
        # Wait for the world to tick to ensure the pedestrians are spawned correctly
        self.world.tick()

    def spawn_pedestrians(self, n_pedestrians: int) -> None:
        """
        Tries to spawn `n_pedestrians` randomly around the city. The number of spawned pedestrians
        might be lower than `n_pedestrians` due to collisions.
        """
        walker_controller_bp = self.blueprint_library.find('controller.ai.walker')
        for _ in range(n_pedestrians):
            walker_bp = random.choice(self.blueprint_library.filter('walker.*'))
            transform = carla.Transform(self.world.get_random_location_from_navigation())
            pedestrian = self.world.try_spawn_actor(walker_bp, transform)
            if pedestrian is None:
                continue
            self.pedestrian_list.append(pedestrian)
            ai_controller = self.world.spawn_actor(walker_controller_bp, carla.Transform(),
                                                   attach_to=pedestrian)
            self.ai_controller_list.append(ai_controller)
        # Wait for the world to tick to ensure the pedestrians are spawned correctly
        self.world.tick()

    def move_pedestrians(self) -> None:
        """
        Move pedestrians to random locations. Randomness is added to the walking speeds
        of the pedestrians.
        """
        for ai_controller in self.ai_controller_list:
            ai_controller.start()
            ai_controller.go_to_location(self.world.get_random_location_from_navigation())
            ai_controller.set_max_speed(1 + random.random())

    def spawn_vehicle(self, vehicle_id: int, spawn_point_id: Optional[int]=None) -> object:
        """
        Spawn a vehicle to a predefined spawn point. The spawn points can be visualized
        using `visualize_spawn_points.py`. If `spawn_point_id` is not given, the vehicle is
        spawned to a random location.
        """
        if spawn_point_id is None:
            spawn_point = random.choice(self.spawn_points)
        else:
            spawn_point = self.spawn_points[spawn_point_id]
        vehicle_bp = self.blueprint_library.find(vehicle_id)
        vehicle = self.world.spawn_actor(vehicle_bp, spawn_point)
        self.vehicle_list.append(vehicle)
        vehicle_id = f'vehicle_{len(self.vehicle_list)}'
        return vehicle

    def spawn_vehicles(self, n_vehicles: int) -> None:
        """
        Tries to spawn `n_vehicles` randomly around the city. The number of spawned vehicles
        might be lower than `n_vehicles` due to collisions.
        """
        for _ in range(n_vehicles):
            vehicle_bp = random.choice(self.blueprint_library.filter('vehicle.*'))
            spawn_point = random.choice(self.spawn_points)
            vehicle = self.world.try_spawn_actor(vehicle_bp, spawn_point)
            if vehicle is None:
                continue
            self.vehicle_list.append(vehicle)

    def create_transform(self, location_tuple: tuple, rotation_tuple: tuple) -> object:
        """Create CARLA transform object using `location_tuple` and `rotation_tuple`."""
        location = carla.Location(location_tuple[0], location_tuple[1], location_tuple[2])
        rotation = carla.Rotation(rotation_tuple[0], rotation_tuple[1], rotation_tuple[2])
        return carla.Transform(location, rotation)

    def spawn_camera(self, location_tuple: tuple, rotation_tuple: tuple=(0, 0, 0),
                     vehicle: Optional[object]=None) -> object:
        """
        Spawn camera to a predefined location. If `vehicle` is not None, the location is
        relative to the vehicle location.
        """
        transform = self.create_transform(location_tuple, rotation_tuple)
        camera = self.world.spawn_actor(self.camera_bp, transform, attach_to=vehicle)
        self.camera_sensor_list.append(camera)
        return camera

    def spawn_lidar(self, location_tuple: tuple, rotation_tuple: tuple=(0, 0, 0),
                    vehicle: Optional[object]=None) -> object:
        """
        Spawn camera to a predefined location. If `vehicle` is not None, the location is
        relative to the vehicle location.
        """
        transform = self.create_transform(location_tuple, rotation_tuple)
        lidar = self.world.spawn_actor(self.lidar_bp, transform, attach_to=vehicle)
        self.lidar_sensor_list.append(lidar)
        return lidar

    def set_autopilot(self) -> None:
        """Set autopilot on for all spawned vehicles."""
        for vehicle in self.vehicle_list:
            vehicle.set_autopilot(True)

    def set_route(self, vehicle: object, route_indices: list) -> None:
        """
        Create a predefined route for `vehicle`. The `route_indices` can be found
        using `visualize_spawn_points.py`.
        """
        route = []
        for i in route_indices:
            route.append(self.spawn_points[i].location)
        self.traffic_manager.set_path(vehicle, route)

    def generate_waypoints(self) -> list:
        """
        Generate waypoints of the simulated world to create a visualization in
        the discrete-event simulation.
        """
        waypoints = []
        waypoints_list = self.map.generate_waypoints(1.0)
        for waypoint in waypoints_list:
            location = waypoint.transform.location
            waypoint_tuple = (location.x, location.y)
            waypoints.append(waypoint_tuple)
        return waypoints

    def clear_actors(self) -> None:
        """Destroy all cameras, vehicles and pedestrians after simulation."""
        for sensor in self.camera_sensor_list:
            try:
                sensor.destroy()
                print("Destroyed actor")
            except:
                print("Could not destroy actor")
        for vehicle in self.vehicle_list:
            try:
                vehicle.destroy()
                print("Destroyed actor")
            except:
                print("Could not destroy actor")
        for ai_controller in self.ai_controller_list:
            try:
                ai_controller.destroy()
                print("Destroyed actor")
            except:
                print("Could not destroy actor")
        for pedestrian in self.pedestrian_list:
            try:
                pedestrian.destroy()
                print("Destroyed actor")
            except:
                print("Could not destroy actor")

    def set_original_settings(self) -> None:
        """Set the CARLA simulator back to asynchronous mode."""
        self.world.apply_settings(self.original_settings)
