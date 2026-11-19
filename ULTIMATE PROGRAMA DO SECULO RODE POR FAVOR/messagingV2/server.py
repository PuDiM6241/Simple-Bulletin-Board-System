import json
import os
import socket
import time
import uuid

import zmq
from google.protobuf.message import DecodeError

import configGeneralData

# ------------------
#       config
# ------------------

serverName = os.getenv("SERVER_NAME", socket.gethostname())
peerPort = os.getenv("PEER_PORT", "5560")
dataDirectory = os.path.join(os.getenv("DATA_DIR", "/data"), serverName)
heartbeatEvery = 10   # mensagens de cliente entre batimentos cardíacos para o processo de referência
syncEvery = 15        # mensagens de cliente entre sincronização de relógio com o coordenador

context = zmq.Context()
clock = configGeneralData.heartbeatClock()

clientSocket = context.socket(zmq.REP)
clientSocket.connect(configGeneralData.brokerRepAddress)
peerSocket = context.socket(zmq.REP)
peerSocket.bind(f"tcp://*:{peerPort}")
pubSocket = context.socket(zmq.PUB)
pubSocket.connect(configGeneralData.proxySubAddress)
subSocket = context.socket(zmq.SUB)
subSocket.connect(configGeneralData.proxyPubAddress)
subSocket.setsockopt(zmq.SUBSCRIBE, b"")

users = {}            # username -> {"firstLogin", "lastLogin"}
channels = []         # [{"name", "createdBy", "timestamp"}]
messages = []         # [{"channel", "username", "text", "timestamp", "logicalClock"}]
modifications = []    # toda a modificacao feita ordenada por timestamp
knownRecordIds = set()

#self
myRank = 0
coordinator = ""
electionRequested = False
availableServers = [] #[(name, rank)]
clientMessageCount = 0

#endereco em string de cada servidor, comunicacao entre servidores
def peerAddress(name):
    return os.getenv("PEER_ADDRESS", f"tcp://{name}:5560")

# ------------------
#     jsonDB
# ------------------

def loadJson(fileName, default):
    try:
        with open(os.path.join(dataDirectory, fileName), encoding="utf-8") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def saveJson(fileName, data):
    os.makedirs(dataDirectory, exist_ok=True)
    with open(os.path.join(dataDirectory, fileName), "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def loadData():
    global users, channels, messages, modifications
    users = loadJson("users.json", {})
    channels = loadJson("channels.json", [])
    messages = loadJson("messages.json", [])
    modifications = loadJson("modifications.json", [])
    knownRecordIds.update(record["id"] for record in modifications)
    configGeneralData.logEvent(serverName, f"Carregados: {len(users)} usuários, {len(channels)} canais e {len(messages)} mensagens")


def channelNames():
    return [channel["name"] for channel in channels]


# ------------------
#     replication
# ------------------

def applyRecord(record):
    data = record["data"]
    if record["kind"] == "login":
        user = users.setdefault(data["username"], {"firstLogin": data["timestamp"], "lastLogin": data["timestamp"]})
        user["firstLogin"] = min(user["firstLogin"], data["timestamp"])
        user["lastLogin"] = max(user["lastLogin"], data["timestamp"])
        saveJson("users.json", users)
    elif record["kind"] == "channel":
        if data["name"] not in channelNames():
            channels.append(data)
            saveJson("channels.json", channels)
    elif record["kind"] == "message":
        messages.append(data)
        saveJson("messages.json", messages)


def storeRecord(record):
    applyRecord(record)
    modifications.append(record)
    modifications.sort(key=lambda item: item["timestamp"])
    knownRecordIds.add(record["id"])
    saveJson("modifications.json", modifications)


def saveLocalRecord(kind, data):
    record = {"id": str(uuid.uuid4()), "kind": kind, "data": data, "timestamp": configGeneralData.now()}
    storeRecord(record)
    protoPackage = configGeneralData.pb.package(id=record["id"], kind=kind, data=json.dumps(data, ensure_ascii=False), timestamp=record["timestamp"])
    envelope = configGeneralData.makeEnvelope(
        clock=clock,
        sender=serverName,
        msgType="replicate",
        topic="replication",
        packages=[protoPackage]
    )
    configGeneralData.sendEnvelope(pubSocket, serverName, envelope)


def applyRemoteRecords(envelope):
    for protoPackage in envelope.packages:
        if protoPackage.id not in knownRecordIds:
            storeRecord({"id": protoPackage.id, "kind": protoPackage.kind,
                         "data": json.loads(protoPackage.data), "timestamp": protoPackage.timestamp})


# ------------------
#     requests
# ------------------

def reply(status, error="", *, channels=None, referenceTime=None):
    return configGeneralData.makeEnvelope(
        clock=clock,
        sender=serverName,
        msgType="response",
        status=status,
        error=error,
        channels=channels,
        referenceTime=referenceTime
    )

def login(request):
    if not request.username:
        return reply("error", "nome de usuário obrigatório")
    saveLocalRecord("login", {"username": request.username, "timestamp": configGeneralData.now()})
    return reply("success")

def createChannel(request):
    name = request.channel.strip("\"").strip()
    if not name:
        return reply("error", "o nome do canal deve estar entre aspas duplas")
    if name in channelNames():
        return reply("error", "o canal já existe")
    saveLocalRecord("channel", {"name": name, "createdBy": request.username, "timestamp": configGeneralData.now()})
    return reply("success")


def listChannels(request):
    return reply("success", channels=channelNames())


def publish(request):
    name = request.channel.strip("\"").strip()
    if not name or name not in channelNames():
        return reply("error", "o canal não existe")
    saveLocalRecord("message", {"channel": name, "username": request.username, "text": request.text,
                                "timestamp": request.timestamp, "logicalClock": request.logicalClock})
    published = configGeneralData.makeEnvelope(
        clock=clock,
        sender=serverName,
        msgType="message",
        timestamp=request.timestamp,
        topic=name,
        username=request.username,
        channel=name,
        text=request.text
    )
    configGeneralData.sendEnvelope(pubSocket, serverName, published)
    return reply("success")


requestTypes = {"login": login, "createChannel": createChannel,
                  "listChannels": listChannels, "publish": publish}


def clientRequest(request):
    requestType = requestTypes.get(request.type)
    if requestType is None:
        return reply("error", "solicitação desconhecida")
    return requestType(request)


# ------------------
#     reference
# ------------------

def askReference(msgType):
    return configGeneralData.request(
        context=context,
        address=configGeneralData.referenceAddress,
        envelope=configGeneralData.makeEnvelope(
            clock=clock,
            sender=serverName,
            msgType=msgType
        ),
        name=serverName,
        clock=clock
    )


def registerWithReference():
    global myRank
    while True:
        answer = askReference("register")
        if answer is not None and answer.status == "success":
            myRank = answer.rank
            configGeneralData.logEvent(serverName, f"Registrado com classificação {myRank}")
            return
        time.sleep(1)


def refreshServers():
    global availableServers
    answer = askReference("listServers")
    if answer is not None and answer.status == "success":
        availableServers = [(info.name, info.rank) for info in answer.servers]


def sendHeartbeat():
    answer = askReference("heartbeat")
    if answer is None:
        return
    if answer.status != "success":
        registerWithReference()    
        return
    if coordinator == "":
        configGeneralData.adjustTime(answer.referenceTime)
    refreshServers()
    if coordinator == serverName:
        announceCoordinator()      
    elif coordinator not in [name for name, rank in availableServers]:
        startElection()            


# ------------------
#     Election
# ------------------

def announceCoordinator():
    announcement = configGeneralData.makeEnvelope(
        clock=clock,
        sender=serverName,
        msgType="coordinator",
        topic="servers",
        coordinator=serverName
    )
    configGeneralData.sendEnvelope(pubSocket, serverName, announcement)


def startElection():
    global coordinator
    refreshServers()
    coordinator = ""
    higherServerAnswered = False
    for name, rank in availableServers:
        if rank > myRank:
            answer = configGeneralData.request(
                context=context,
                address=peerAddress(name),
                envelope=configGeneralData.makeEnvelope(
                    clock=clock,
                    sender=serverName,
                    msgType="election"
                ),
                name=serverName,
                clock=clock
            )
            if answer is not None:
                higherServerAnswered = True
    if not higherServerAnswered:
        coordinator = serverName
        configGeneralData.logEvent(serverName, "Sou o novo coordenador")
        announceCoordinator()


# ------------------
#     berkeleySync
# ------------------

def berkeleyRound(requesterName="", requesterTime=0.0):
    refreshServers()
    differences = [0.0]    
    for name, rank in availableServers:
        if name == serverName or name == requesterName:
            continue
        sentAt = configGeneralData.now()
        answer = configGeneralData.request(
            context=context,
            address=peerAddress(name),
            envelope=configGeneralData.makeEnvelope(
                clock=clock,
                sender=serverName,
                msgType="time"
            ),
            name=serverName,
            clock=clock
        )
        if answer is not None:
            receivedAt = configGeneralData.now()
            differences.append(answer.timestamp - (sentAt + receivedAt) / 2)
    if requesterName:
        differences.append(requesterTime - configGeneralData.now())
    adjustment = sum(differences) / len(differences)
    
    configGeneralData.adjustTime(configGeneralData.now() + adjustment)

    configGeneralData.logEvent(serverName, f"Berkeley: diferenças={[round(d, 3) for d in differences]} ajuste={adjustment:.3f}s")
    return configGeneralData.now()


def syncClock():
    if coordinator == "":
        return
    if coordinator == serverName:
        berkeleyRound()
        return
    sentAt = configGeneralData.now()
    answer = configGeneralData.request(
        context=context,
        address=peerAddress(coordinator),
        envelope=configGeneralData.makeEnvelope(
            clock=clock,
            sender=serverName,
            msgType="clock"
        ),
        name=serverName,
        clock=clock,
        timeoutMs=5000
    )
    if answer is None or answer.status != "success":
        startElection()
        return
    configGeneralData.adjustTime(answer.referenceTime + (configGeneralData.now() - sentAt) / 2)
    configGeneralData.logEvent(serverName, f"Relógio sincronizado com o coordenador {coordinator}")


# ------------------
#    Requests
# ------------------

def handlePeerRequest(request):
    global electionRequested
    clock.shync(request.logicalClock)
    configGeneralData.logMessage(serverName, "RECEBIDO", request)
    if request.type == "election":
        electionRequested = True
        answer = reply("success")
    elif request.type == "time":
        answer = configGeneralData.makeEnvelope(
            clock=clock,
            sender=serverName,
            msgType="time"
        )
    elif request.type == "clock" and coordinator == serverName:
        correctTime = berkeleyRound(request.sender, request.timestamp)
        answer = reply("success", referenceTime=correctTime)
    else:
        answer = reply("error", "solicitação não suportada")
    configGeneralData.sendEnvelope(peerSocket, serverName, answer)


# ------------------
#     subscription
# ------------------

def subscription(data):
    global coordinator
    try:
        envelope = configGeneralData.parseEnvelope(data)
    except DecodeError:
        return
    if envelope.sender == serverName: #ele nao precisa consumir suas proprias mensagens
        return
    clock.shync(envelope.logicalClock)
    configGeneralData.logMessage(serverName, "RECEBIDO", envelope)
    if envelope.type == "coordinator":
        coordinator = envelope.coordinator
    elif envelope.type == "replicate":
        applyRemoteRecords(envelope)


# ------------------
#     mainLoop
# ------------------

loadData()
registerWithReference()
startElection()

poller = zmq.Poller()
poller.register(clientSocket, zmq.POLLIN)
poller.register(peerSocket, zmq.POLLIN)
poller.register(subSocket, zmq.POLLIN)

configGeneralData.logEvent(serverName, "Servidor em execução")
while True:
    events = dict(poller.poll(1000))

    if subSocket in events:
        subscription(subSocket.recv())

    if peerSocket in events:
        handlePeerRequest(configGeneralData.parseEnvelope(peerSocket.recv()))

    if clientSocket in events:
        clientEnvelope = configGeneralData.parseEnvelope(clientSocket.recv())
        clock.shync(clientEnvelope.logicalClock)
        configGeneralData.logMessage(serverName, "RECEBIDO", clientEnvelope)
        try:
            answer = clientRequest(clientEnvelope)
        except Exception as error:    # a REP socket must always send a reply
            answer = reply("error", str(error))
        configGeneralData.sendEnvelope(clientSocket, serverName, answer)
        clientMessageCount += 1
        if clientMessageCount % heartbeatEvery == 0:
            sendHeartbeat()
        if clientMessageCount % syncEvery == 0:
            syncClock()

    if electionRequested:
        electionRequested = False
        startElection()