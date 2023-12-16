from kafka import KafkaProducer
import json, socket, os, re, signal, sys
from dotenv import load_dotenv

# Load ENV variables
load_dotenv()
TWITCH_SERVER = "irc.chat.twitch.tv"
TWITCH_PORT = 6667
twitch_nickname = os.getenv('TWITCH_NICKNAME')
twitch_channel = os.getenv('TWITCH_CHANNEL')
twitch_oauth = os.getenv('TWITCH_OAUTH')
aiven_host = os.getenv('AIVEN_HOST')
aiven_port = os.getenv('AIVEN_PORT')


## Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers= f"{aiven_host}:{aiven_port}",
    security_protocol="SSL",
    ssl_cafile="./ca.pem",
    ssl_certfile="./service.cert",
    ssl_keyfile="./service.key",
    value_serializer=lambda v: json.dumps(v).encode('ascii')
)

# set up message socket from twitch 
sock = socket.socket()
sock.connect((TWITCH_SERVER,TWITCH_PORT))
sock.send(f"PASS {twitch_oauth}\n".encode('utf-8'))
sock.send(f"NICK {twitch_nickname}\n".encode('utf-8'))
sock.send(f"JOIN {twitch_channel}\n".encode('utf-8'))


# elegant exiting service, since there is an infinite loop
def cleanup_before_exit(signum, frame):
    print("\nGracefully closing the script...")
    producer.flush()
    sock.close()
    print("Cleanup completed.")
    sys.exit(0)


# reused function to parse messages from chat
def parse_chat(resp): 
    search = re.search(":(.*)\!.*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)", resp)
    if search:
        username, channel, message  = search.groups()
        print(f"Channel: {channel} \nUsername: {username} \nMessage: {message}")
        return username, channel, message
    return None, None, None

signal.signal(signal.SIGINT, cleanup_before_exit)

try:
    while True:
        resp = sock.recv(2048).decode('utf-8')
        print(resp)
        multimessage = resp.split('\r\n')
        if len(multimessage)==1:
            username, chat, message = parse_chat(resp)
            if username != None:
                producer.send('chatmessages',key=bytes(username, "utf-8"), value= f'{message}')
        else:
            for twitch_input in multimessage:
                username, chat, message = parse_chat(twitch_input)
                if username != None:
                    producer.send('chatmessages', key=bytes(username, "utf-8"), value=f'{message}')

        if resp.startswith('PING'):
            sock.send("PONG\n".encode('utf-8'))
except Exception as e:
    print(e)
    print(f"An error occurred: {str(e)}")
    cleanup_before_exit(signal.SIGINT, None)
