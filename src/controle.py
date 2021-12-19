import sys
sys.path.append('..')
from is_wire.rpc import ServiceProvider, LogInterceptor
from is_wire.core import Channel, Message, Subscription, StatusCode, Status, Logger
from google.protobuf.empty_pb2 import Empty
from google.protobuf.struct_pb2 import Struct
from is_msgs.common_pb2 import Position
from is_msgs.robot_pb2 import RobotTaskRequest
import json
from time import sleep

class Robot():
    def __init__(self, id, x, y):
        self.id = id
        self.pos_x = x
        self.pos_y = y

        provider.delegate(
            topic=f"Controle.Get.Position.Robo.{id}",
            function=getPosition,
            request_type=Struct,
            reply_type=Position) 

        provider.delegate(
            topic=f"Controle.Set.Position.Robo.{id}",
            function=setPosition,
            request_type=RobotTaskRequest,
            reply_type=Empty)
    
    def getId(self):
        return self.id
    
    def setPosition(self, x, y):
        self.pos_x = x
        self.pos_y = y

    def getPosition(self):
        return self.pos_x, self.pos_y

def getRobot(list_of_robot,id):
    for robot in list_of_robot:
        if int(robot.getId()) == int(id):
            log.info(f'Robot {id} found.')
            return robot
    return None           

def initRobots():
    with open('../config/robot_list.json', 'r') as json_file:
        robot_config_list = json.load(json_file)
    robot_list = []
    for robot in robot_config_list:
        id = robot['id']
        x = robot['x']
        y = robot['y']
        log.info(f'ROBOT {id}: -x: {x} -y: {y}')
        robot = Robot(id=id, x=x, y=y)
        robot_list.append(robot)
        sleep(0.5)
    return robot_list

def getPosition(struct_id,ctx):
    log.info('GET POSITION request received from OPERATOR...')
    sleep(0.5)
    id = struct_id.fields['id'].string_value
    robot = getRobot(robots,id)
    log.info('Validating arguments...')
    sleep(0.5)
    if robot is not None:
        robot_request = Position()
        robot_request.x, robot_request.y = robot.getPosition()
        log.info(f'ROBOT ID {robot.getId()} -X: {robot_request.x}  -Y: {robot_request.y}')
        log.info('Sending GET POSITION reply...')
        sleep(1)
        return robot_request

    else:
        log.error(f"Robot {id} not found")
        return Status(StatusCode.NOT_FOUND, "ROBOT ID not found.")

def setPosition(request_msg,ctx):
    log.info('SET POSITION request received from OPERATOR...')
    sleep(0.5)
    id = request_msg.id
    robot = getRobot(robots,id)
    log.info('Validating arguments...')
    sleep(0.5)
    if request_msg.basic_move_task.positions[0].x < 0 or request_msg.basic_move_task.positions[0].y < 0:
        return Status(StatusCode.OUT_OF_RANGE, "The number must be positive")

    if robot is not None:
        log.info(f'Moving ROBOT ID {robot.getId()} to X: {request_msg.basic_move_task.positions[0].x} and Y: {request_msg.basic_move_task.positions[0].y}')
        robot.setPosition(x=request_msg.basic_move_task.positions[0].x, y=request_msg.basic_move_task.positions[0].y)
        log.info('Sending SET POSITION reply...')
        sleep(1)
        return Status(StatusCode.OK, f"Robot {robot.getId()} successfully moved.")

    else:
        log.error(f"Robot {id} not found")
        return Status(StatusCode.NOT_FOUND, "ROBOT ID not found.")

channel = Channel("amqp://guest:guest@localhost:5672")
subscription = Subscription(channel)
provider = ServiceProvider(channel)
logging = LogInterceptor()
provider.add_interceptor(logging)
log = Logger(name='controle')

robots = initRobots()
provider.run()