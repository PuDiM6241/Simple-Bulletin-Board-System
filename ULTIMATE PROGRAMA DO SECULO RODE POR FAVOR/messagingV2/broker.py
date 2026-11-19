import zmq

# ------------------
#     reqRepBroker
# ------------------
# 5555, porta brooker para clientes (REQ)
# 5556, porta brooker para servidores (REP)

context = zmq.Context()

clientSocket = context.socket(zmq.ROUTER)
clientSocket.bind("tcp://*:5555")
serverSocket = context.socket(zmq.DEALER)
serverSocket.bind("tcp://*:5556")

print("broker em execução: clientes 5555, servidores 5556", flush=True)
zmq.proxy(clientSocket, serverSocket)
