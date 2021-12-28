import sys
sys.path.append('..')
from is_wire.rpc import ServiceProvider, LogInterceptor
from is_wire.core import Channel, Message, Subscription, StatusCode, Status, Logger
from is_msgs.common_pb2 import Position
from is_msgs.robot_pb2 import RobotTaskRequest
from google.protobuf.struct_pb2 import Struct
from msgs.RequisicaoRobo_pb2 import RequisicaoRobo
import random
import json
import socket
from time import sleep

def turnSysOnline():
    provider.delegate(
        topic="Requisicao.Robo",
        function=requisicaoRobo,
        request_type=RequisicaoRobo,
        reply_type=RequisicaoRobo)
    provider.run()

def requisicaoRobo(request_msg,ctx):
    log.info(f'{request_msg.function} request received from OPERATOR...')
    sleep(0.5)

    log.info(f'Sending {request_msg.function} request to ROBOT CONTROLLER...')
    sleep(0.5)
    if request_msg.function == 'get_position':
        robot_id = request_msg.id
        struct_id = Struct()
        struct_id.update({"id": f'{robot_id}'})
        get_position_request = Message(content=struct_id, reply_to=subscription)
        channel.publish(get_position_request, topic=f"Controle.Get.Position.Robo.{robot_id}")
        
        try:
            log.info(f'Waiting {request_msg.function} reply from ROBOT CONTROLLER...')
            sleep(1)
            reply = channel.consume(timeout=5.0)

            if reply.status is None:
                return Status(StatusCode.NOT_FOUND, "ROBOT ID topic not found.")
            else:
                if str(reply.status.code) == 'StatusCode.OK':
                    reply_msg = reply.unpack(Position)
                    
                    log.info(f'ROBOT {robot_id} POSITION: -x: {reply_msg.x} -y: {reply_msg.y}')

                    user_reply_msg = RequisicaoRobo()
                    user_reply_msg.id = robot_id
                    user_reply_msg.function = request_msg.function
                    user_reply_msg.positions.x = reply_msg.x
                    user_reply_msg.positions.y = reply_msg.y

                    return user_reply_msg
                else:
                    return reply.status

        except socket.timeout:
            log.warn('Connection Problem with Robot Controller.')

    elif request_msg.function == 'set_position':
        robot_id = request_msg.id
        set_request_msg = RobotTaskRequest()
        set_request_msg.id = robot_id
        set_request_msg.basic_move_task.positions.append(Position(x=request_msg.positions.x, y=request_msg.positions.y))

        set_position_request = Message(content=set_request_msg, reply_to=subscription)
        channel.publish(set_position_request, topic=f"Controle.Set.Position.Robo.{robot_id}")
        
        try:
            log.info(f'Waiting {request_msg.function} reply from ROBOT CONTROLLER...')
            sleep(1)
            reply = channel.consume(timeout=5.0)
            
            if reply.status is None:
                return Status(StatusCode.NOT_FOUND, "ROBOT ID topic not found.")
            else:
                return reply.status
        except socket.timeout:
            log.warn('Connection Problem with Robot Controller.')

    else:
        return Status(StatusCode.UNKNOWN, "Unknown function.")

def sysInitMsg():
    while True:
        random_number = random.randrange(0, 2)
        userMessage = channel.consume()
        userMessage = userMessage.body.decode('latin1')
        if userMessage == "Ligar Sistema":
            log.info('Message Received. Checking content and trying to bring the system online...')
            sleep(1)
            if random_number == 1:
                log.info('SYSTEM ONLINE')
                notifyMessage = Message()
                notifyMessage.body = "Sistema Ligado".encode('latin1')
                channel.publish(notifyMessage, topic="Controle.Console.Robot")
                turnSysOnline()
                break
            else:
                log.warn("Failed to bring the system online.")
                log.info("Sending notification to User...")
                notifyMessage = Message()
                notifyMessage.body = "Failed to bring the system online.".encode('latin1')
                channel.publish(notifyMessage, topic="Controle.Console.Robot")


config = json.load(open('../config/config.json', 'r'))
channel = Channel(config['broker.channel'])
subscription = Subscription(channel)
subscription.subscribe(topic="Controle.Console.User")
provider = ServiceProvider(channel)
logging = LogInterceptor()
provider.add_interceptor(logging)
log = Logger(name='robot')

log.info(f'Waiting TURN ON message...')
sysInitMsg()