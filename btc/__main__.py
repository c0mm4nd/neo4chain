# %%
import asyncio
import copy
from bitcoinrpc import BitcoinRPC
from py2neo import Graph, SystemGraph, ClientError, Node, Transaction
rpc = BitcoinRPC("127.0.0.1", 18332, "bitcoinrpc",
                 "xauRXtQnwpihPxDQzG2g5Jw1Oaq4NG8W/FBX8CLGfJqK")
sg = SystemGraph("bolt://127.0.0.1:7687", auth=("neo4j", "icaneatglass"))
db = Graph("bolt://127.0.0.1:7687", auth=("neo4j", "icaneatglass"), name="btc")


def create_db():
    sg.run('create database btc')
    # todo: some indexes


try:
    db.match_one(None)
except ClientError:
    create_db()

# %%


def save_block(t: Transaction, block: dict):
    pure_block = copy.deepcopy(block)
    pure_block.pop("hex", None)
    pure_block.pop("tx", None)
    pure_block.pop("txid",  None)
    pure_block.pop("previousblockhash", None)
    pure_block.pop("nextblockhash", None)
    pure_block.pop("confirmations", None)
    pure_block.pop("versionHex", None)
    t.merge(Node("Block", **pure_block),
            primary_label=('Block'), primary_key=("hash"))

def save_cb_vin(t: Transaction, vin):
    pass

def save_cb(t: Transaction, tx):
    pure_tx = copy.deepcopy(tx)
    pure_tx.pop('hex')
    pure_tx.pop('vin')
    pure_tx.pop('vout')
    for vin in tx['vin']:
        save_cb_vin(t, vin)
    for vout in tx['vout']:
        save_vout(t, vout)
    pass


def save_tx(t: Transaction, tx):
    # https://developer.bitcoin.org/reference/transactions.html
    pure_tx = copy.deepcopy(tx)
    pure_tx.pop('hex')
    pure_tx.pop('vin')
    pure_tx.pop('vout')
    for vin in tx['vin']:
        save_vin(t, vin)
    for vout in tx['vout']:
        save_vout(t, vout)
    pass


def save_address(t: Transaction, addr):
    pass


async def get_top_height():
    info = await rpc.getblockchaininfo()
    return info['blocks']


async def get_block_by_height(height):
    hash = await rpc.getblockhash(height)
    # verbosity 2 will carry the tx entities
    return await rpc.getblock(hash, 2)


def parse_block(block):
    t = db.auto()
    save_block(t, block)
    is_coinbase = True
    for tx in block['tx']:
        if is_coinbase:
            is_coinbase = False
            save_cb(t, tx)
        save_tx(t, tx)


async def main():
    # c = await get_block_by_height(1)
    block = await get_block_by_height(1)
    # parse_block(block)
    for tx in block["tx"]:
        print(tx)
    print()
    await rpc.aclose()

# %%
if __name__ == "__main__":
    asyncio.run(main())
