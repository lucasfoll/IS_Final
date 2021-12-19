import sys
sys.path.append('..')
from is_wire.core import Channel, Message, Subscription, Logger
from msgs.RequisicaoRobo_pb2 import RequisicaoRobo
import random
import socket
from time import sleep

functions = ['get_position', 'set_position']

log = Logger(name='User')

userMessage = Message()
userMessage.body = "Ligar Sistema".encode('latin1')

channel = Channel("amqp://guest:guest@localhost:5672")
subscription = Subscription(channel)
subscription.subscribe(topic="Controle.Console.Robot")

while True:
    channel.publish(userMessage, topic="Controle.Console.User")
    notifyMessage = channel.consume()
    notifyMessage = notifyMessage.body.decode('latin1')
    log.info(notifyMessage)
    if notifyMessage == "Sistema Ligado":
        break

while True:
    id = int(random.randrange(0, 5))
    x = int(random.randrange(0, 11))
    y = int(random.randrange(0, 11))

    for function in functions:
        log.info(f'Creating {function} request to ROBOT {id}...')
        sleep(0.5)
        robot_request_msg = RequisicaoRobo()
        robot_request_msg.id = id
        robot_request_msg.function = function
        robot_request_msg.positions.x = x
        robot_request_msg.positions.y = y

        if function == 'get_position':
            log.info('Sending a GET POSITION request...')
        else:
            log.info(f'Sending a SET POSITION request for X={x} Y={y}')

        robot_request = Message(content=robot_request_msg, reply_to=subscription)
        channel.publish(robot_request, topic="Requisicao.Robo")

        try:
            log.info(f'Waiting {function} request...')
            reply = channel.consume(timeout=5.0)
            if str(reply.status.code) == 'StatusCode.OK':
                req_robo_reply = reply.unpack(RequisicaoRobo)
                
                if function == 'get_position':
                    log.info(f'GET_POSITION REPLY FOR ROBOT ID {robot_request_msg.id}')
                    log.info(f'x = {req_robo_reply.positions.x}, y = {req_robo_reply.positions.y}')
                else:
                    log.info(f'SET_POSITION REPLY FOR ROBOT ID {robot_request_msg.id}')
            else:
                log.warn(f'{reply.status.code}')

        except socket.timeout:
            log.warn('No reply')

    sleep(1)