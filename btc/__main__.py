# %%
from time import time
from functools import wraps
from threading import Thread
from bitcoin_rpc_client import RPCClient
from neo4j import Driver, GraphDatabase, Session, Transaction
from neo4j.io import ClientError
import os
import logging

logger = logging.getLogger(__name__)

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        if te - ts > 1:
            logger.warning("func: {} took {} sec".format(f.__name__, te-ts))
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
    "txinwitness", 
    "sequence" # The script sequence number
]

# https://bitcoincore.org/en/doc/0.21.0/rpc/rawtransactions/decoderawtransaction/
VIN_NODE_FIELDS = [
    "txid",
    "vout",
    "scriptSig", # keep the hex
    "txinwitness", # null or ","-joint list
    "sequence"
]

VOUT_NODE_FIELDS = [
    "locktime",  # txid is not unique in the chain
    "txid",
    "n",  # index
    "value",
    "address"

    "hex"  # from scriptPubKey.hex by python script
]

ADDRESS_NODE_FIELDS = [
    "address",
    "scriptPubKey",
    "isscript",
    "iswitness",

    "pubkey" # optional, if it is exposed
]

# CoinbaseNode = Node(("Address"), address='')


def create_db(driver):
    system = driver.session()
    system.run('create database btc')
    system.close()

    # todo: some indexes
    db = driver.session(database="btc")
    db.run("CREATE CONSTRAINT ON (c:Coinbase) ASSERT c.id IS UNIQUE;")
    db.run("CREATE CONSTRAINT ON (b:Block) ASSERT b.hash IS UNIQUE;")
    db.run("CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;")
    db.run("CREATE CONSTRAINT ON (o:Output) ASSERT (o.height, o.txid, o.n) IS NODE KEY;")
    db.run("CREATE CONSTRAINT FOR (tx:Transaction) REQUIRE (tx.height, tx.txid) IS UNIQUE;")
    db.run("CREATE INDEX for (tx:Transaction) on (tx.txid);") # txid is not unique
    db.run("CREATE INDEX for (o:Output) on (o.txid, o.n);")
    db.run("CREATE (:Coinbase) ") # coinbase node
    db.close()

def ensure_coinbase_addr_exists(t: Transaction):
    t.run("MERGE (:Coinbase) ")

def ensure_db_exists(driver):
    try:
        db = driver.session(database="btc")
        db.run("create (placeholder:Block {height: -1})")
        db.run("MATCH  (placeholder:Block {height: -1}) delete placeholder")
        ensure_coinbase_addr_exists(db)
        db.close()
    except ClientError as e:
        if e.code == 'Neo.ClientError.Database.DatabaseNotFound':
            create_db(driver)
        else:
            raise e


# %%

@timing
def parse_block(driver: Driver, rpc, block: dict, is_genesis=False):
    co = 100
    multi_thread = block["tx"] is not None and len(block["tx"]) * 10     >= co
    if is_genesis:
        block["previousblockhash"] = None
    with driver.session(database="btc") as session:
        session.run("""
            CREATE (b:Block {
                hash: $hash,
                confirmations: $confirmations,
                height: $height,
                version: $version,
                versionHex: $versionHex,
                merkleroot: $merkleroot,
                time: $time,
                mediantime: $mediantime,
                nonce: $nonce,
                bits: $bits,
                difficulty: $difficulty,
                nTx: $nTx,
                chainwork: $chainwork,
                previousblockhash: $previousblockhash,
                nextblockhash: $nextblockhash,
                strippedsize: $strippedsize,
                size: $size,
                weight: $weight
            })
        """, **block)
        if not multi_thread:
            for tx in block["tx"]:
                session.write_transaction(parse_tx, rpc, block, tx)

    if multi_thread:
        i = 0
        logger.warning("concurrently processing {} txs in block {}".format(len(block["tx"]), block["height"]))
        tasks = [Thread(target=parse_tx_task, args=(driver, rpc, block, tx)) for tx in block["tx"]]
        while i < len(tasks):
            end = i+co if i+co < len(tasks)-1 else len(tasks)-1
            logger.warning(f"processing block tx {i} to {end}")
            for t in tasks[i: end]:
                t.start()
            for t in tasks[i: end]:
                t.join()
            i = i+co

def parse_tx_task(driver, rpc, block, tx):
    with driver.session(database="btc") as session:
        try:
            session.write_transaction(parse_tx, rpc, block, tx)
        except Exception as e:
            logger.error(e)
            os._exit(0)

def parse_tx(t: Transaction, rpc, block, tx):
    # https://developer.bitcoin.org/reference/transactions.html
    if tx.get("fee", None) is None:
        tx["fee"] = None # is None when Coinbase
    t.run("""
        CREATE (:Transaction {
            txid: $txid,
            hash: $hash,
            height: $height,
            version: $version,
            size: $size,
            vsize: $vsize,
            weight: $weight,
            locktime: $locktime,
            fee: $fee
        })
    """, **tx, height=block["height"]) # add height, because locktime is useless 
    # solve vins and vouts
    for vin in tx['vin']:
        if vin.get('coinbase', None) is not None and len(vin['coinbase']) > 0:
            # is a coinbase
            t.run("""
            MATCH (o:Coinbase) 
            WITh o
            CREATE (vin:Input {
                coinbase: $coinbase,
                txinwitness: $txinwitness,
                sequence: $sequence
            })
            CREATE (o)-[:SPENT_BY]->(vin)
            CREATE (vin)-[:VIN]->(:Transaction {txid: $txid})
            """,
            # scriptSig=None if vin.get("scriptSig", None) is None else vin["scriptSig"]["hex"],
            coinbase=vin["coinbase"], 
            txinwitness=None if vin.get("txinwitness", None) is None else ",".join(vin["txinwitness"]),
            sequence=vin["sequence"], txid=tx["txid"])
        else:
            t.run("""
            MATCH (o:Output {txid: $out_txid, n: $out_n}) 
            WITh o
            CREATE (vin:Input {
                // txid: txid,
                // vout: vout,
                scriptSig: $scriptSig,
                txinwitness: $txinwitness,
                sequence: $sequence
            })
            CREATE (o)-[:SPENT_BY]->(vin)
            CREATE (vin)-[:VIN]->(:Transaction {txid: $txid})
            """, out_txid=vin['txid'], out_n=vin['vout'], 
            # txid=vin["txid"],
            # vout=vin["out"],
            scriptSig=None if vin.get("scriptSig", None) is None else vin["scriptSig"]["hex"],
            txinwitness=None if vin.get("txinwitness", None) is None else ",".join(vin["txinwitness"]),
            sequence=vin["sequence"], txid=tx["txid"])

    for vout in tx['vout']:
        if vout['scriptPubKey']['type'] in ('nulldata', 'nonstandard'):
            t.run("""
            CREATE (o:Output {
                type: $type,
                height: $height,
                txid: $txid,
                n: $n,
                value: $value,
                hex: $hex
            })
            CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
            """, type=vout['scriptPubKey']['type'], height=block["height"],
            txid=tx["txid"], n=vout["n"], value=vout["value"], hex=vout['scriptPubKey']['hex'])
        elif vout['scriptPubKey']['type'] in ('pubkeyhash', 'scripthash'):
            save_address(t, vout['scriptPubKey']['address'])
            t.run("""
            MATCH (a:Address {address: $address})
            WITH a
            CREATE (o:Output {
                type: "address",
                height: $height,
                txid: $txid,
                n: $n,
                value: $value
            })
            CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
            CREATE (o)-[:TO]->(a)
            """, height=block["height"],
            txid=tx["txid"], n=vout["n"], value=vout["value"],
            address = vout['scriptPubKey']['address']
            )
        elif vout['scriptPubKey']['type'] == 'pubkey':
            public_key = vout['scriptPubKey']['asm'].split(" ")[0]
            descriptor = rpc.getdescriptorinfo(f"pkh({public_key})")['descriptor']
            addr = rpc.deriveaddresses(descriptor)[0]
            save_address(t, addr, vout['scriptPubKey']['hex'])
            t.run("""
            MATCH (a:Address {address: $address})
            WITH a
            CREATE (o:Output {
                type: "address",
                height: $height,
                txid: $txid,
                n: $n,
                value: $value
            })
            CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
            CREATE (o)-[:TO]->(a)
            """, height=block["height"],
            txid=tx["txid"], n=vout["n"], value=vout["value"],
            address = addr
            )
        else:
            logger.error('unknown type: "{}" on tx {}'.format(
                vout["scriptPubKey"]["type"], tx["txid"]))
            exit(1)
    t.run("""
    MATCH (b:Block {hash: $hash}), (t:Transaction {txid: $txid})
    WITH b,t
    CREATE (b)-[:CONTAINS]->(t)
    """, hash=block["hash"], txid=tx["txid"])


# def get_vout_from_db(t, txid, n):
#     out = t.run("",
#                 txid=txid, n=n).evaluate()
#     if out is None:
#         logger.error("ERROR: no output found for txid: {} n: {}".format(txid, n))
#         exit(1)
#     return out


def save_address(t: Transaction, addr, public_key=None):
    result = rpc.validateaddress(addr)
    if not result['isvalid']:
        logger.error("invalid address: " + addr)
        exit(1)
    else:
        t.run("""
        MERGE (a:Address {
            address: $address,
            scriptPubKey: $scriptPubKey,
            isscript: $isscript,
            iswitness: $iswitness
        })
        """, **result)
        if public_key is not None:
            t.run("""
            MATCH (a:Address {address: $address})
            WHERE a.pubkey IS NULL
            SET a.pubkey = $pubkey
            RETURN count(a)
            """, address=addr, pubkey=public_key).value()

# %%

def get_remote_top_height(rpc):
    info = rpc.getblockchaininfo()
    return info['blocks']


def get_block_by_height(rpc, height):
    hash = rpc.getblockhash(height)
    # verbosity 2 will carry the tx entities
    return rpc.getblock(hash, 2)


def get_local_top_block(driver: Driver):
    with driver.session(database="btc") as session:
        results = session.run("MATCH (b:Block) RETURN b order by b.height desc limit 1;").value()
        if results is None or len(results) == 0:
            return None
        return results[0]

def supplement_missing(driver, block):
    height = block["height"]
    with driver.session(database="btc") as session:
        results = session.run("MATCH (b:Block {height: $height}) RETURN b", height=height).value()
        if results is None or len(results) == 0:
            logger.error(f"cannot find block {height}")
            logger.error(results)
            os._exit(0)
    # checking txs
    tx_number = len(block["tx"])
    logger.warning(f"checking missing tx({tx_number} in total ) for block {height}")
    tasks = [Thread(target=supplement_missing_tx_task, args=(driver, rpc, block, tx)) for tx in block["tx"]]
    for t in tasks:
        t.start()
    for i, t in enumerate(tasks):
        t.join()
        logger.warning(f"checked no.{i} tx in block")
    logger.warning("check finished")


def supplement_missing_tx_task(driver, rpc, block, tx):
    with driver.session(database="btc") as session:
        txid = tx["txid"]
        results = session.run("MATCH (tx:Transaction {txid: $txid}) RETURN count(tx)", txid=txid).value()
        if results is None or len(results) == 0 or results[0] == 0:
            logger.warning(f"txid {txid} is not found, supplement...")
            session.write_transaction(parse_tx, rpc, block, tx)
            logger.warning(f"txid {txid} is supplemented")


def work_flow(driver, rpc):
    remote_top = get_remote_top_height(rpc)
    target = (remote_top - 1000) // 1000 * 1000
    top_block = get_local_top_block(driver)

    local_height = 0

    if top_block is not None:
        supplement_missing(driver, get_block_by_height(rpc, top_block["height"]))
        local_height = top_block['height']
    else:
        logger.warning("processing genesis block")
        # session.write_transaction(parse_block, rpc, get_block_by_height(rpc, 0), True)
        parse_block(driver, rpc, get_block_by_height(rpc, 0), True)

    for i in range(local_height + 1, target):
        block = get_block_by_height(rpc, i)
        logger.warning("processing block(with {} txs): {} -> {}(remote {})".format(len(block["tx"]), i, target, remote_top))        
        # session.write_transaction(parse_block, rpc, block)
        parse_block(driver, rpc, block)
        # if top_node is not None:
        #     t.create(Relationship(block_node, "PREV", top_node))


if __name__ == "__main__":
    # getting ready
    logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
    logger.setLevel(logging.INFO)

    rpc = RPCClient("http://127.0.0.1:8332", "bitcoin", "password", parse_float=float)
    driver = GraphDatabase.driver(
        "bolt://127.0.0.1:7687", auth=("neo4j", "icaneatglass"))
    
    ensure_db_exists(driver)

    work_flow(driver, rpc)
