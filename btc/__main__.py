# %%
import argparse
import json
from time import time
from functools import wraps
from threading import Thread
from bitcoin_rpc_client import RPCClient
from neo4j import Driver, GraphDatabase, Session, Transaction
from neo4j.io import ClientError
from concurrent.futures import ThreadPoolExecutor
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
            logger.debug("func: {} took {} sec".format(f.__name__, te-ts))
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
    "sequence"  # The script sequence number
]

# https://bitcoincore.org/en/doc/0.21.0/rpc/rawtransactions/decoderawtransaction/
VIN_NODE_FIELDS = [
    "txid",
    "vout",
    "scriptSig",  # keep the hex
    "txinwitness",  # null or ","-joint list
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

    "pubkey"  # optional, if it is exposed
]

# CoinbaseNode = Node(("Address"), address='')


class BitcoinETL:
    def __init__(self, config):
        self.config = config
        rpc_config = config["daemon"]  # required
        neo4j_config = config["neo4j"]  # required

        self.rpc = RPCClient(
            rpc_config["address"], rpc_config["username"], rpc_config["password"], parse_float=float)
        self.driver = GraphDatabase.driver(
            neo4j_config["address"], auth=(neo4j_config["username"], neo4j_config["password"]))
        self.dbname = neo4j_config.get("database", "btc")

        self.ensure_db_exists()

    def create_db(self):
        with self.driver.session() as system:
            system.run('create database btc')

        # todo: some indexes
        with self.driver.session(database=self.dbname) as session:
            session.run(
                "CREATE CONSTRAINT ON (c:Coinbase) ASSERT c.id IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (b:Block) ASSERT b.hash IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (o:Output) ASSERT (o.height, o.txid, o.n) IS NODE KEY;")
            session.run(
                "CREATE CONSTRAINT FOR (tx:Transaction) REQUIRE (tx.height, tx.txid) IS UNIQUE;")
            # txid is not unique
            session.run("CREATE INDEX for (tx:Transaction) on (tx.txid);")
            session.run("CREATE INDEX for (o:Output) on (o.txid, o.n);")
            session.run("CREATE (:Coinbase) ")  # coinbase node

    def ensure_db_exists(self):
        try:
            with self.driver.session(database=self.dbname) as session:
                session.run("create (placeholder:Block {height: -1})")
                session.run(
                    "MATCH  (placeholder:Block {height: -1}) delete placeholder")
                # ensure_coinbase_addr_exists
                session.run("MERGE (:Coinbase) ")
        except ClientError as e:
            if e.code == 'Neo.ClientError.Database.DatabaseNotFound':
                self.create_db()
            else:
                raise e

    @timing
    def parse_block(self, block, executor=None, is_genesis=False):
        if is_genesis:
            block["previousblockhash"] = None
        with self.driver.session(database="btc") as session:
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

        i = 0
        logger.warning("concurrently processing {} txs in block {}".format(
                len(block["tx"]), block["height"]))
        [executor.submit(self.parse_tx_task, block, tx) for tx in block["tx"]]
        

    def parse_tx_task(self, block, tx):
        with self.driver.session(database="btc") as session:
            try:
                session.write_transaction(self.parse_tx, block, tx)
            except Exception as e:
                logger.error(e)
                os._exit(0)

    def parse_tx(self, t, block, tx):
        # https://developer.bitcoin.org/reference/transactions.html
        if tx.get("fee") is None:
            tx["fee"] = None  # is None when Coinbase
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
        """, **tx, height=block["height"])  # add height, because locktime is useless
        # solve vins and vouts
        for vin in tx['vin']:
            if vin.get('coinbase') is not None and len(vin['coinbase']) > 0:
                # is a coinbase
                t.run("""
                MATCH (o:Coinbase) 
                WITH o
                CREATE (vin:Input {
                    coinbase: $coinbase,
                    txinwitness: $txinwitness,
                    sequence: $sequence
                })
                CREATE (o)-[:SPENT_BY]->(vin)
                CREATE (vin)-[:VIN]->(:Transaction {txid: $txid})
                """,
                      # scriptSig=None if vin.get("scriptSig") is None else vin["scriptSig"]["hex"],
                      coinbase=vin["coinbase"],
                      txinwitness=None if vin.get(
                          "txinwitness") is None else ",".join(vin["txinwitness"]),
                      sequence=vin["sequence"], txid=tx["txid"])
            else:
                t.run("""
                MATCH (o:Output {txid: $out_txid, n: $out_n}) 
                WITH o
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
                      scriptSig=None if vin.get(
                          "scriptSig") is None else vin["scriptSig"]["hex"],
                      txinwitness=None if vin.get(
                          "txinwitness") is None else ",".join(vin["txinwitness"]),
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
                self.save_address(t, vout['scriptPubKey']['address'])
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
                      address=vout['scriptPubKey']['address']
                      )
            elif vout['scriptPubKey']['type'] == 'pubkey':
                public_key = vout['scriptPubKey']['asm'].split(" ")[0]
                descriptor = self.rpc.getdescriptorinfo(
                    f"pkh({public_key})")['descriptor']
                addr = self.rpc.deriveaddresses(descriptor)[0]
                self.save_address(t, addr, vout['scriptPubKey']['hex'])
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
                      address=addr
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

    def save_address(self, t, addr, public_key=None):
        result = self.rpc.validateaddress(addr)
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

    def get_remote_top_height(self):
        info = self.rpc.getblockchaininfo()
        return info['blocks']

    def get_block_by_height(self, height):
        hash = self.rpc.getblockhash(height)
        # verbosity 2 will carry the tx entities
        return self.rpc.getblock(hash, 2)

    def get_local_top_block(self, tx):
        results = tx.run(
            "MATCH (b:Block) RETURN b order by b.height desc limit 1;").value()
        if results is None or len(results) == 0:
            return None
        return results[0]

    def supplement_missing(self, block):
        height = block["height"]
        with self.driver.session(database="btc") as session:
            results = session.run(
                "MATCH (b:Block {height: $height}) RETURN b", height=height).value()
            if results is None or len(results) == 0:
                logger.error(f"cannot find block {height}")
                logger.error(results)
                os._exit(0)
        # checking txs
        tx_number = len(block["tx"])
        logger.warning(
            f"checking missing tx({tx_number} in total) for block {height}")
        tasks = [Thread(target=self.supplement_missing_tx_task,
                        args=(block, tx)) for tx in block["tx"]]
        for t in tasks:
            t.start()
        for t in tasks:
            t.join()

    def block_tx_exists(self, t, height, txid):
        results = t.run(
            "MATCH (tx:Transaction {height: $height, txid: $txid}) RETURN count(tx)", height=height, txid=txid).value()
        return results is not None and len(results) != 0 and results[0] != 0

    def supplement_missing_tx_task(self, block, tx):
        with self.driver.session(database=self.dbname) as session:
            height = block["height"]
            txid = tx["txid"]
            if not session.read_transaction(self.block_tx_exists, height, txid):
                logger.warning(
                    f"txid {txid} on {height} is not found, supplement...")
                session.write_transaction(self.parse_tx, block, tx)
                logger.warning(f"txid {txid} on {height} is supplemented")

    def work_flow(self):
        remote_top = self.get_remote_top_height()
        target = (remote_top - 1000) // 1000 * 1000

        with self.driver.session(database=self.dbname) as session:
            top_block = session.read_transaction(self.get_local_top_block)

        local_height = 0
        if top_block is not None:
            local_height = top_block['height']
        else:
            logger.warning("processing genesis block")
            self.parse_block(self.get_block_by_height(0), is_genesis=True)

        if self.config.get("checker") is not None and local_height > 0:
            co = self.config["checker"].get("thread", 100)
            logger.warning(f'running on check missing mode, thread {co}')
            safe_height = self.config["checker"].get("safe-height")
            if safe_height is None:
                self.supplement_missing(
                    self.get_block_by_height(top_block["height"]))
            else:
                end = top_block["height"]
                logger.warning(f"checking from {safe_height} to {end}")
                def task(height):
                    block = self.get_block_by_height(height)
                    self.supplement_missing(block)
                with ThreadPoolExecutor(co) as executor:
                    executor.map(task, range(safe_height+1, end))

        if self.config.get("syncer") is not None and local_height < target:
            co = self.config["syncer"].get("thread", 100)
            logger.warning(f'running on sync mode, thread {co}')
            with ThreadPoolExecutor(co) as executor:
                for i in range(local_height + 1, target):
                    block = self.get_block_by_height(i)
                    logger.warning("processing block(with {} txs): {} -> {}(remote {})".format(
                        len(block["tx"]), i, target, remote_top))
                    # session.write_transaction(parse_block, rpc, block)
                    self.parse_block(block, executor=executor)
                # if top_node is not None:
                #     t.create(Relationship(block_node, "PREV", top_node))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-c", "--config-file", type=str,
                        default="config.json", help="The config file")
    args = parser.parse_args()

    filepath = args.config_file

    # getting ready
    logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S')
    logger.setLevel(logging.INFO)

    with open(filepath) as file:
        config = json.load(file)
    btc_config = config["btc"]

    etl = BitcoinETL(config=btc_config)
    etl.work_flow()
