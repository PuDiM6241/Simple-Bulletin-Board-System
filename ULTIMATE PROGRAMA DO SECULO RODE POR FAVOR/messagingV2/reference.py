import time

import zmq

import configGeneralData 

#Server de referência, que mantém o ranking dos servidores ativos e responde a requisições de registro, listagem e heartbeat.


# ------------------
#     config
# ------------------

referenceName = "reference"
heartbeatTimeout = 30   #(30 segundos sem heartbeat) server removido do ranking
context = zmq.Context()
clock = configGeneralData.heartbeatClock()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")
servers = {}
nextRank = 1


def removeSilentServers():
    for name in list(servers):
        if time.time() - servers[name]["lastSeen"] > heartbeatTimeout:
            del servers[name]
            configGeneralData.logEvent(referenceName, f"servidor {name} removido (sem heartbeat há {heartbeatTimeout}s)")

def reply(status, *, error=None, rank=None, servers=None, referenceTime=None):
    return configGeneralData.makeEnvelope(
        clock=clock,
        sender=referenceName,
        msgType="response",
        status=status,
        error=error,
        rank=rank,
        servers=servers,
        referenceTime=referenceTime
    )

def Request(request):
    global nextRank

    if request.type == "register":
        if request.sender not in servers:
            servers[request.sender] = {"rank": nextRank, "lastSeen": time.time()}
            nextRank += 1
        servers[request.sender]["lastSeen"] = time.time()
        return reply("success", rank=servers[request.sender]["rank"])

    if request.type == "listServers":
        ordered = sorted(servers.items(), key=lambda item: item[1]["rank"])
        return reply("success", servers=[configGeneralData.pb.serverInfo(name=name, rank=info["rank"]) for name, info in ordered])

    if request.type == "heartbeat":
        if request.sender not in servers:
            return reply("error", error="servidor desconhecido")
        servers[request.sender]["lastSeen"] = time.time()
        return reply("success", referenceTime=time.time())

    return reply("error", error="solicitação desconhecida")


# ------------------
#     mainLoop
# ------------------

configGeneralData.logEvent(referenceName, "Processo de referência em execução na porta 5559")
while True:
    if socket.poll(1000):
        request = configGeneralData.parseEnvelope(socket.recv())
        clock.shync(request.logicalClock)
        configGeneralData.logMessage(referenceName, "RECEBIDO", request)
        removeSilentServers()
        configGeneralData.sendEnvelope(socket, referenceName, Request(request))
    else:
        removeSilentServers()
