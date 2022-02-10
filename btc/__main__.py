# %%
from time import time
from functools import wraps
import os
from decimal import Decimal
from bitcoin_rpc_client import RPCClient
from py2neo import Graph, SystemGraph, ClientError, Node, Transaction, Relationship
rpc = RPCClient("http://127.0.0.1:8332", "bitcoin", "password")
sg = SystemGraph("bolt://127.0.0.1:7687", auth=("neo4j", "icaneatglass"))
db = Graph("bolt://127.0.0.1:7687",
           auth=("neo4j", "icaneatglass"), name="btc")


if "DEBUG" in os.environ and os.environ["DEBUG"] != "":
    import logging
    import sys
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        level=logging.DEBUG,
    )


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print("func: {} took {} sec".format(f.__name__, te-ts))
        return result
    return wrap


BLOCK_NODE_FIELDS = [
    "hash",
    # "confirmations",
    "height",
    "version",
    "versionHex",
    "merkleroot",
    "time",
    "mediantime",
    "nonce",
    "bits",
    "difficulty",
    # "nTx", // use count() instead
    "chainwork",
    "previousblockhash",
    # "nextblockhash",
    "strippedsize",
    "size",
    "weight",
]

TX_NODE_FIELDS = [
    "txid",
    "hash",
    "version",
    "size",
    "vsize",
    "weight",
    "locktime",
    "fee",
]

CB_VIN_NODE_FIELDS = [
    "coinbase",
    # "txinwitness",
    "sequence"
]

VIN_NODE_FIELDS = [
    "coinbase",
    # "txinwitness",
    "sequence"
]

VOUT_NODE_FIELDS = [
    "locktime",  # txid is not unique in the chain
    "txid",
    "n",  # index
    "value",
    "address"

    "asm"  # from scriptPubKey.asm by python script
    "hex"  # from scriptPubKey.hex by python script
]

ADDRESS_NODE_FIELDS = [
    "address",
    "isscript",
    "iswitness",
]

CoinbaseNode = Node(("Address"), address='')


def create_db():
    sg.run('create database btc')
    # todo: some indexes
    db.run("CREATE CONSTRAINT ON (b:Block) ASSERT b.hash IS UNIQUE;")
    db.run("CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;")
    db.run("CREATE CONSTRAINT ON (o:Output) ASSERT (o.height, o.txid, o.n) IS NODE KEY;")


def ensure_coinbase_addr_exists():
    db.merge(CoinbaseNode, 'Address', "address")


try:
    db.match_one(None)
except ClientError:
    create_db()

ensure_coinbase_addr_exists()

# %%


def keep_fields_from_obj(obj, fields):
    new_obj = {}
    for field in fields:
        value = obj.get(field, None)
        if value is None:
            continue
        if type(value) is Decimal:
            new_obj[field] = str(value)
        else:
            new_obj[field] = value
    return new_obj


def save_and_return_block(t: Transaction, block: dict):
    pure_block = keep_fields_from_obj(block, BLOCK_NODE_FIELDS)
    block_node = Node("Block", **pure_block)
    t.create(block_node)
    for tx in block["tx"]:
        tx_node = save_and_return_tx(t, block, tx)
        t.create(Relationship(block_node, "COINTAINS", tx_node))
    return block_node


def save_and_return_tx(t: Transaction, block, tx):
    # https://developer.bitcoin.org/reference/transactions.html
    pure_tx = keep_fields_from_obj(tx, TX_NODE_FIELDS)
    tx_node = Node("Transaction", **pure_tx)
    t.create(tx_node)
    # solve vins and vouts
    for vin in tx['vin']:
        if vin.get('coinbase', None) is not None and len(vin['coinbase']) > 0:
            # is a coinbase
            parent_node = CoinbaseNode
            vin_node_fields = keep_fields_from_obj(vin, CB_VIN_NODE_FIELDS)
        else:
            parent_node = get_vout_from_db(t, vin['txid'], vin['vout'])
            vin_node_fields = keep_fields_from_obj(vin, VIN_NODE_FIELDS)
        vin_node = Node("Input", **vin_node_fields)
        t.create(Node("Input", **vin_node))
        t.create(Relationship(parent_node, "SPENT_BY", vin_node))
        t.create(Relationship(vin_node, "VIN", tx_node))
    for vout in tx['vout']:
        vout_node_fields = keep_fields_from_obj(vout, VOUT_NODE_FIELDS)
        vout_node = Node("Output", **vout_node_fields)
        vout_node["height"] = block["height"]
        vout_node["txid"] = tx["txid"]

        address_node = None
        if vout['scriptPubKey']['type'] in ('nulldata', 'nonstandard'):
            vout_node['type'] = vout['scriptPubKey']['type']
            vout_node['asm'] = vout['scriptPubKey']['asm']
            vout_node['hex'] = vout['scriptPubKey']['hex']
        elif vout['scriptPubKey']['type'] == 'pubkeyhash':
            vout_node['type'] = "address"
            address_node = save_and_return_address(
                t, vout['scriptPubKey']['address'])
        elif vout['scriptPubKey']['type'] == 'pubkey':
            vout_node['type'] = "address"
            descriptor = rpc.getdescriptorinfo("pkh({})".format(
                vout['scriptPubKey']['asm'].split(" ")[0]))['descriptor']
            addr = rpc.deriveaddresses(descriptor)[0]
            address_node = save_and_return_address(t, addr)
        else:
            print('unknown type: "{}" on tx {}'.format(
                vout["scriptPubKey"]["type"], tx["txid"]))
            exit(1)
        t.create(vout_node)
        t.create(Relationship(tx_node, "VOUT", vout_node))
        if address_node is not None:
            t.create(Relationship(vout_node, "TO", address_node))
    return tx_node


def get_vout_from_db(t, txid, n):
    out = t.run("MATCH (t:Transaction {txid: $txid})-[:VOUT]->(o:Output) WHERE o.n = $n RETURN o",
                txid=txid, n=n).evaluate()
    if out is None:
        print("ERROR: no output found for txid: {} n: {}".format(txid, n))
        exit(1)
    return out


def save_and_return_address(t: Transaction, addr):
    result = rpc.validateaddress(addr)
    if not result['isvalid']:
        print("invalid address: " + addr)
        exit(1)
    else:
        addr_fields = keep_fields_from_obj(result, ADDRESS_NODE_FIELDS)
        addr_node = Node("Address", **addr_fields)
        t.merge(addr_node, "Address", "address")
        return addr_node


def get_remote_top_height():
    info = rpc.getblockchaininfo()
    return info['blocks']


def get_block_by_height(height):
    hash = rpc.getblockhash(height)
    # verbosity 2 will carry the tx entities
    return rpc.getblock(hash, 2)


def get_local_top_block():
    return db.query("MATCH (b:Block) RETURN b order by b.height desc limit 1;").evaluate()


def main():
    remote_top = get_remote_top_height()
    target = (remote_top - 1000) // 1000 * 1000
    top_node = get_local_top_block()
    local_height = 0
    if top_node is not None:
        local_height = top_node['height']
    else:
        print("processing genesis block")
        t = db.begin()
        top_node = save_and_return_block(t, get_block_by_height(0))
        db.commit(t)
    for i in range(local_height + 1, target):
        print("processing block: {} -> {}({})".format(i, target, remote_top))
        block = get_block_by_height(i)
        t = db.begin()
        block_node = save_and_return_block(t, block)
        if top_node is not None:
            t.create(Relationship(block_node, "PREV", top_node))
        db.commit(t)
        top_node = block_node


# %%
if __name__ == "__main__":
    main()
