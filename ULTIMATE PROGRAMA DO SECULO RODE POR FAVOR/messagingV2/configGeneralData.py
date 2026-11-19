
import datetime
import os
import time

import zmq

import messaging_pb2 as pb  # pacotes protobuf das mensagens

# ------------------
#     network
# ------------------


#Tava dando erro usando local host porque eu crio mais de um container, isso resolveu servers com problemas
brokerReqAddress = os.getenv("BROKER_REQ_ADDRESS", "tcp://broker:5555")     
brokerRepAddress = os.getenv("BROKER_REP_ADDRESS", "tcp://broker:5556")
proxySubAddress = os.getenv("PROXY_SUB_ADDRESS", "tcp://proxy:5557")        
proxyPubAddress = os.getenv("PROXY_PUB_ADDRESS", "tcp://proxy:5558")        
referenceAddress = os.getenv("REFERENCE_ADDRESS", "tcp://reference:5559")   


# ---------------------
# Time synchronization
# ---------------------


#Sincronisacao usa variavel de offset.
physicalClockOffset = 0.0

#Usar isso para todos os tempos
def now():
    return time.time() + physicalClockOffset

def adjustTime(correctTime):
    global physicalClockOffset
    physicalClockOffset += correctTime - now()

#Odeio classe de phyton
class heartbeatClock:
    def __init__(self):
        self.counter = 0

    #Auto incremento
    def tick(self):
        self.counter += 1
        return self.counter

    #Implementacao do algoritmo de barkley
    def shync(self, receivedClock):
        self.counter = max(self.counter, receivedClock)


# ------------------------------
#   Sistema de Log no console
# ------------------------------

def logEvent(name, text):
    timestamp = datetime.datetime.fromtimestamp(now()).strftime("%H:%M:%S.%f")
    print(f"[{name}] \n[{timestamp}] \n{text}", flush=True)

def logMessage(name, direction, envelope, textExtra="", extra=None):
    if extra is not None:
        textExtra = extra
    log = []
    
    for field, value in envelope.ListFields():
        if field.name in ("timestamp", "referenceTime"):
            value = datetime.datetime.fromtimestamp(value).strftime("%H:%M:%S.%f")
        elif field.name == "channels":
            value = list(value)
        elif field.name == "servers":
            value = [f"nome:{server.name} | classificacao:{server.rank}" for server in value]
        elif field.name == "packages":
            value = [f"id:{package.id} | tipo:{package.kind} | dados:{package.data} | timestamp:{package.timestamp}" for package in value]

        nomesCampos = {
            "type": "tipo",
            "sender": "remetente",
            "timestamp": "timestamp",
            "logicalClock": "relógioLogico",
            "status": "status",
            "error": "erro",
            "username": "usuario",
            "channel": "canal",
            "text": "texto",
            "channels": "canais",
            "rank": "classificacao",
            "servers": "servidores",
            "coordinator": "coordenador",
            "referenceTime": "tempoReferencia",
            "packages": "pacotes",
            "topic": "topico"
        }
        nomeCampo = nomesCampos.get(field.name, field.name)
        log.append(f"{nomeCampo}={value!r}")
    
    timestamp = datetime.datetime.fromtimestamp(now()).strftime("%H:%M:%S.%f")
    envelopeLog = "\n".join(log)
    print(
        f"[{name}] [{timestamp}] {direction} {envelope.type} {envelopeLog} {textExtra}".rstrip(),
        flush=True,
    )


# ------------------
#   mensage trader
# ------------------
# me arrependo de nao ter feito em c#, sinto falta de classes decentes.

def makeEnvelope(
    clock, sender, msgType,
    timestamp=None,
    status=None,
    topic=None,
    error=None,
    username=None,
    channel=None,
    text=None,
    channels=None,
    rank=None,
    servers=None,
    coordinator=None,
    referenceTime=None,
    packages=None
):
    envelope = pb.envelope(
        type=msgType,
        sender=sender,
        timestamp=now() if timestamp is None else timestamp,
        logicalClock=clock.tick()
    )

    optionalFields = {
        "status": status,
        "topic": topic,
        "error": error,
        "username": username,
        "channel": channel,
        "text": text,
        "rank": rank,
        "coordinator": coordinator,
        "referenceTime": referenceTime
    }

    for field, value in optionalFields.items():
        if value is not None:
            setattr(envelope, field, value)

    if channels is not None:
        envelope.channels.extend(channels)

    if servers is not None:
        envelope.servers.extend(servers)

    if packages is not None:
        envelope.packages.extend(packages)

    return envelope


def parseEnvelope(data):
    envelope = pb.envelope()
    envelope.ParseFromString(data)
    return envelope


def sendEnvelope(socket, name, envelope):
    data = envelope.SerializeToString()
    socket.send(data)
    logMessage(name, "||ENVIADO||\n", envelope)


def request(context, address, envelope, name, clock, timeoutMs=5000):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    try:
        socket.connect(address)
        sendEnvelope(socket, name, envelope)
        if not socket.poll(timeoutMs):  #sistema de timeout, se nao receber resposta em 5s, retorna None
            return None
        reply = parseEnvelope(socket.recv())
        clock.shync(reply.logicalClock)
        logMessage(name, "||RECEBIDO||\n", reply)
        return reply
    finally:
        socket.close()
