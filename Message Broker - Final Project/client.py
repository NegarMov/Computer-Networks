"""
Message Broker Client

@author: Negar Movaghatian - 9831062
"""

import sys
import socket
import threading
import datetime
import random

MESSAGE_LENGTH_SIZE = 64
ENCODING = 'utf-8'
ID = str(int(random.random() * 1000000))

finished = False

def check_args(expected):
    if len(sys.argv) < expected:
        print("ERR: Too few arguments.")
        exit()

def main():
    check_args(4)
    HOST = sys.argv[1]
    PORT = int(sys.argv[2])
    CMD  = sys.argv[3]
    SERVER_INFO = (HOST, PORT)

    if CMD not in ["publish", "subscribe", "ping"]:
        print("ERR: Invalid command.")
        exit()

    mt = threading.Thread(target=manage, args=(SERVER_INFO,))
    mt.start()

    if CMD == "publish":
        check_args(6)
        TOPIC = sys.argv[4]
        MSG   = sys.argv[5]

        # print(f"[INFO] publishing <{TOPIC} - {MSG}>")
        publish(SERVER_INFO, TOPIC, MSG)

    elif CMD == "subscribe":
        check_args(5)
        TOPICS = sys.argv[4:]

        # print(f"[INFO] subscribing to {TOPICS}")
        for topic in TOPICS:
            t = threading.Thread(target=subscribe, args=(SERVER_INFO, topic))
            t.start()

    elif CMD == "ping":
        ping(SERVER_INFO)

def publish(SERVER_INFO, TOPIC, MSG):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        global finished

        try:
            s.connect(SERVER_INFO)
        except:
            print(f"unable to reach server at {SERVER_INFO}.")
            finished = True
            return

        send_msg(s, ID)
        send_msg(s, "Publish")
        send_msg(s, TOPIC)
        send_msg(s, MSG)

        s.settimeout(10)
        try:
            ack = recv_msg(s)
            if ack == "PubACK":
                print("your message published successfully")
            else:
                raise Exception("unknown ACK received")
        except:
            print("your message publishing failed")
        
        finished = True


def subscribe(SERVER_INFO, TOPIC):
    global finished

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(SERVER_INFO)
        except:
            print(f"unable to reach server at {SERVER_INFO}.")
            finished = True
            return

        send_msg(s, ID)
        send_msg(s, "Subscribe")
        send_msg(s, TOPIC)

        s.settimeout(10)
        try:
            ack = recv_msg(s)
            if ack == "SubACK":
                print(f"subscribed to {TOPIC} successfully")
            else:
                raise Exception("unknown ACK received")
        except:
            print(f"subscribing failed for topic {TOPIC}")

        s.settimeout(None)
        while True:
            try:
                cmd = recv_msg(s)
                if cmd == "Message":
                    msg = recv_msg(s)
                    print(f"** New Message ** - {TOPIC}: {msg}")
                else:
                    raise Exception("unknown command")    
            except:
                print("an error was encountered while communicating with the server or the connection is closed.")
                break
        
        finished = True

def ping(SERVER_INFO):
    global finished

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        start = datetime.datetime.now()

        try:
            s.connect(SERVER_INFO)
        except:
            print(f"ERR: unable to reach server at {SERVER_INFO}.")
            return

        send_msg(s, ID)
        send_msg(s, "Ping")

        s.settimeout(10)
        try:
            pong = recv_msg(s)
            if pong == "Pong":
                end = datetime.datetime.now()
                elapsed_time = (end - start).total_seconds() * 1000
                print(f"successfully pinged {SERVER_INFO} in {elapsed_time} milliseconds.")
            else:
                raise Exception("unknown response received")
        except:
            print(f"failed to ping {SERVER_INFO}")

        finished = True

def manage(SERVER_INFO):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(SERVER_INFO)
        except:
            return

        send_msg(s, ID)
        send_msg(s, "Manage")

        while not finished:
            try:
                ping = recv_msg(s)
                if ping == "Ping":
                    send_msg(s, "Pong")
                else:
                    raise Exception("unknown response received")
            except:
                break

def recv_msg(client):
    msg_length = int(client.recv(MESSAGE_LENGTH_SIZE).decode(ENCODING))
    msg = client.recv(msg_length).decode(ENCODING)
    return msg

def send_msg(client, message):
    msg = message.encode(ENCODING)

    msg_length = len(msg)
    msg_length = str(msg_length).encode(ENCODING)
    msg_length += b' ' * (MESSAGE_LENGTH_SIZE - len(msg_length))

    client.send(msg_length)
    client.send(msg)

if __name__ == '__main__':
    main()