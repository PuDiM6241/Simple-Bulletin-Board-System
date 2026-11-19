import zmq

# ------------------
#     pubSubProxy
# ------------------
# 5557, porta proxy para publishers (SUB)
# 5558, porta proxy para subscribers (PUB)

context = zmq.Context()

publisherSocket = context.socket(zmq.XSUB)
publisherSocket.bind("tcp://*:5557")
subscriberSocket = context.socket(zmq.XPUB)
subscriberSocket.bind("tcp://*:5558")

print("proxy em execução: XSUB 5557, XPUB 5558", flush=True)
zmq.proxy(publisherSocket, subscriberSocket)
