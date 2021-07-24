import asyncio

from blockchain.blockchain import Blockchain
from blockchain.connections import ConnectionPool
from blockchain.peers import P2PProtocol
from blockchain.server import Server

blockchain = Blockchain()
connection_pool = ConnectionPool()

server = Server(blockchain, connection_pool, P2PProtocol)

async def main():
    await server.listen()

asyncio.run(main())