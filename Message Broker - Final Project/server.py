"""
Message Broker Server

@author: Negar Movaghatian - 9831062
"""

import socket
from sys import argv
import threading
import time

HOST = "127.0.0.1"
PORT = 1373
MESSAGE_LENGTH_SIZE = 64
ENCODING = 'utf-8'

publishing_clients = []
subscribed_clients = {}
pinging_clients    = []

def main():
    HOST_INFO = (HOST, PORT)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(HOST_INFO)
        start(s)

def start(server):
    print("[SERVER START]   server is starting...")

    server.listen()

    while True:
        conn, address = server.accept()
        t = threading.Thread(target=client_handler, args=(conn, address))
        t.start()

def client_handler(conn, address):
    id  = recv_msg(conn)
    cmd = recv_msg(conn)

    print(f"[NEW CONNECTION] connected from {address} - {cmd}")

    match cmd:
        case "Publish":
            topic = recv_msg(conn)
            msg = recv_msg(conn)
            print(f"[NEW MESSAGE]    from {address} - {id} - <{topic} : {msg}>")

            publishing_clients.append((conn, id))

            if topic in subscribed_clients.keys():
                for sc_conn, i in reversed(subscribed_clients[topic]):
                    try:
                        send_msg(sc_conn, "Message")
                        send_msg(sc_conn, msg)
                    except:
                        for c, i in reversed(subscribed_clients[topic]):
                            if i == id:
                                c.close()
                                subscribed_clients[topic].remove((c, i))

            send_msg(conn, "PubACK")

        case "Subscribe":
            topic = recv_msg(conn)

            if not topic in subscribed_clients.keys():
                subscribed_clients[topic] = []
            subscribed_clients[topic].append((conn, id))

            send_msg(conn, "SubACK")

        case "Ping":
            pinging_clients.append((conn, id))
            send_msg(conn, "Pong")

        case "Manage":
            client_manager(conn, id, address)
        
        case default:
            print(f"[ERR]            invalid command from client {address}")
            conn.close()

def client_manager(conn, id, address):
    failed_ping_count = 0

    conn.settimeout(10)
    while True:
        start = time.time()

        try:
            send_msg(conn, "Ping")
            while recv_msg(conn) != "Pong":
                pass
            failed_ping_count = 0
        except:
            failed_ping_count += 1
            print(f"[PING FAILED]     failed to ping client {address} (#{failed_ping_count}).")
            if failed_ping_count == 3:
                print(f"[CLOSING CLIENT]  3 failed pings. closing client {address}.")
                for c, i in reversed(publishing_clients):
                    if i == id:
                        c.close()
                        publishing_clients.remove((c, i))
                for c, i in reversed(pinging_clients):
                    if i == id:
                        c.close()
                        pinging_clients.remove((c, i))
                for topic in subscribed_clients:
                    for c, i in reversed(subscribed_clients[topic]):
                        if i == id:
                            c.close()
                            subscribed_clients[topic].remove((c, i))
                conn.close()
                break


        end = time.time()
        time.sleep(10 - (end - start))


def recv_msg(conn):
    msg_length = int(conn.recv(MESSAGE_LENGTH_SIZE).decode(ENCODING))
    msg = conn.recv(msg_length).decode(ENCODING)
    return msg

def send_msg(conn, message):
    msg = message.encode(ENCODING)

    msg_length = len(msg)
    msg_length = str(msg_length).encode(ENCODING)
    msg_length += b' ' * (MESSAGE_LENGTH_SIZE - len(msg_length))

    conn.send(msg_length)
    conn.send(msg)

if __name__ == '__main__':
    main()