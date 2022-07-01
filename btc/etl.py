# %%
from bitcoin_rpc_client import RPCClient
from neo4j import GraphDatabase
from neo4j.io import ClientError
from concurrent.futures import ThreadPoolExecutor, wait
import os
import logging

logger = logging.getLogger(__name__)


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
            system.run(f'create database {self.dbname}')

        # todo: some indexes
        with self.driver.session(database=self.dbname) as session:
            session.run(
                "CREATE CONSTRAINT ON (c:Coinbase) ASSERT c.id IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (b:Block) ASSERT b.hash IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (b:Block) ASSERT b.height IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;")
            session.run(
                "CREATE CONSTRAINT ON (o:Output) ASSERT (o.height, o.txid, o.n) IS NODE KEY;")
            session.run(
                # CREATE CONSTRAINT FOR (a:A) REQUIRE (a.b, a.c) IS UNIQUE requires neo4j 4.4
                "CREATE CONSTRAINT ON (tx:Transaction) ASSERT (tx.height, tx.txid) IS NODE KEY;")
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

    def parse_block(self, block, executor=None, is_genesis=False):
        if is_genesis:
            block["previousblockhash"] = None
        with self.driver.session(database=self.dbname) as session:
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

        if executor is None:
            [self.parse_tx_task(block, tx) for tx in block["tx"]]
        else:
            logger.warning("concurrently processing {} txs in block {}".format(
                len(block["tx"]), block["height"]))
            wait([executor.submit(self.parse_tx_task, block, tx)
                 for tx in block["tx"]])

    def parse_tx_task(self, block, tx):
        with self.driver.session(database="btc") as session:
            # try:
            session.write_transaction(self.parse_tx, block, tx)
            # except Exception as e:
            #     logger.error(e)
            #     os._exit(0)

    def parse_tx(self, t, block, tx):
        # https://developer.bitcoin.org/reference/transactions.html
        query = """
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
        """  # add height, because locktime is useless
        if tx.get("fee") is None:
            t.run(query, **tx, height=block["height"], fee=None)
        else:
            t.run(query, **tx, height=block["height"])

        # solve vins and vouts
        for vin in tx['vin']:
            if vin.get('coinbase') is not None and len(vin['coinbase']) > 0:
                # is a coinbase
                t.run("""
                MATCH (o:Coinbase), (tx:Transaction {txid: $txid, height: $height})
                WITH o,tx
                CREATE (vin:Input {
                    coinbase: $coinbase,
                    txinwitness: $txinwitness,
                    sequence: $sequence
                })
                CREATE (o)-[:SPENT_BY]->(vin)
                CREATE (vin)-[:VIN]->(tx)
                """,
                      # scriptSig=None if vin.get("scriptSig") is None else vin["scriptSig"]["hex"],
                      txid=tx["txid"], height=block["height"],
                      coinbase=vin["coinbase"],
                      txinwitness=None if vin.get(
                          "txinwitness") is None else ",".join(vin["txinwitness"]),
                      sequence=vin["sequence"], )
            else:
                t.run("""
                MATCH (o:Output {txid: $out_txid, n: $out_n}), (tx:Transaction {txid: $txid, height: $height})
                WITH o,tx
                limit 1
                CREATE (vin:Input {
                    // txid: txid,
                    // vout: vout,
                    scriptSig: $scriptSig,
                    txinwitness: $txinwitness,
                    sequence: $sequence
                })
                CREATE (o)-[:SPENT_BY]->(vin)
                CREATE (vin)-[:VIN]->(tx)
                """,
                      out_txid=vin['txid'], out_n=vin['vout'],
                      txid=tx["txid"], height=block["height"],
                      # txid=vin["txid"],
                      # vout=vin["out"],
                      scriptSig=None if vin.get(
                          "scriptSig") is None else vin["scriptSig"]["hex"],
                      txinwitness=None if vin.get(
                          "txinwitness") is None else ",".join(vin["txinwitness"]),
                      sequence=vin["sequence"])

        for vout in tx['vout']:
            if vout['scriptPubKey']['type'] in ('nulldata', 'nonstandard'):
                t.run("""
                MATCH (tx:Transaction {txid: $txid, height: $height})
                WITH tx
                CREATE (o:Output {
                    type: $type,
                    height: $height,
                    txid: $txid,
                    n: $n,
                    value: $value,
                    hex: $hex
                })
                CREATE (tx)-[:VOUT]->(o)
                """, type=vout['scriptPubKey']['type'], height=block["height"],
                      txid=tx["txid"], n=vout["n"], value=vout["value"], hex=vout['scriptPubKey']['hex'])
            elif vout['scriptPubKey']['type'] in ('pubkeyhash', 'scripthash'):
                self.save_address(t, vout['scriptPubKey']['address'])
                t.run("""
                MATCH (a:Address {address: $address}),(tx:Transaction {txid: $txid})
                WITH a,tx
                CREATE (o:Output {
                    type: "address",
                    height: $height,
                    txid: $txid,
                    n: $n,
                    value: $value
                })
                CREATE (tx)-[:VOUT]->(o)
                CREATE (o)-[:TO]->(a)
                """, height=block["height"], txid=tx["txid"],
                      n=vout["n"], value=vout["value"],
                      address=vout['scriptPubKey']['address']
                      )
            elif vout['scriptPubKey']['type'] == 'pubkey':
                public_key = vout['scriptPubKey']['asm'].split(" ")[0]
                descriptor = self.rpc.getdescriptorinfo(
                    f"pkh({public_key})")['descriptor']
                addr = self.rpc.deriveaddresses(descriptor)[0]
                self.save_address(
                    t, addr, public_key=vout['scriptPubKey']['hex'])
                t.run("""
                MATCH (a:Address {address: $address}),(tx:Transaction {txid: $txid, height: $height})
                WITH a,tx
                CREATE (o:Output {
                    type: "address",
                    height: $height,
                    txid: $txid,
                    n: $n,
                    value: $value
                })
                CREATE (tx)-[:VOUT]->(o)
                CREATE (o)-[:TO]->(a)
                """, height=block["height"], txid=tx["txid"],
                      n=vout["n"], value=vout["value"],
                      address=addr
                      )
            elif vout['scriptPubKey']['type'] == 'multisig':
                asms = vout['scriptPubKey']['asm'].split(" ")
                public_keys = [publicKey for publicKey in asms if publicKey.startswith(
                    "04") or publicKey.startswith("03") or publicKey.startswith("02")]
                public_keys_required = int(asms[0])
                assert public_keys_required <= len(public_keys)
                multisig_address = self.rpc.createmultisig(public_keys_required, public_keys)[
                    "address"]  # legacy address (p2sh)
                self.save_address(t, multisig_address,
                                  multisig=vout['scriptPubKey']['asm'])

                for public_key in public_keys:
                    descriptor = self.rpc.getdescriptorinfo(
                        f"pkh({public_key})")['descriptor']
                    addr = self.rpc.deriveaddresses(descriptor)[0]
                    self.save_address(
                        t, addr, public_key=vout['scriptPubKey']['hex'])
                    t.run("""
                    MATCH (multisig:Address {address: $multisig_address}), (sub:Address {address: $sub})
                    WITH multisig, sub
                    CREATE (multisig)-[:CONTROLLED_BY]->(sub)
                    """, multisig_address=multisig_address,
                          sub=addr)

                t.run("""
                MATCH (a:Address {address: $multisig_address}),(tx:Transaction {txid: $txid, height: $height})
                WITH a,tx
                CREATE (o:Output {
                    type: "address",
                    height: $height,
                    txid: $txid,
                    n: $n,
                    value: $value
                })
                CREATE (tx)-[:VOUT]->(o)
                CREATE (o)-[:TO]->(a)
                """, height=block["height"],
                      txid=tx["txid"], n=vout["n"], value=vout["value"],
                      multisig_address=multisig_address
                      )
            else:
                logger.error('unknown type: "{}" on tx {}'.format(
                    vout["scriptPubKey"]["type"], tx["txid"]))
                os._exit(1)

            self.link_block_with_tx(t, block, tx)

    def save_address(self, t, addr, public_key=None, multisig=None):
        result = self.rpc.validateaddress(addr)
        if not result['isvalid']:
            logger.error("invalid address: " + addr)
            os._exit(1)
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
            if multisig is not None:
                t.run("""
                MATCH (a:Address {address: $address})
                WHERE a.multisig IS NULL
                SET a.multisig = $multisig
                RETURN count(a)
                """, address=addr, multisig=multisig).value()

    def get_remote_top_height(self):
        info = self.rpc.getblockchaininfo()
        return info['blocks']

    def get_block_by_height(self, height):
        retry = 0
        while True:
            try:
                hash = self.rpc.getblockhash(height)
                # verbosity 2 will carry the tx entities
                block = self.rpc.getblock(hash, 2)
                return block
            except Exception as e:
                logger.warning(e)
                if retry == 3:
                    os._exit(0)
                retry += 1

    def get_local_top_block(self, tx):
        results = tx.run(
            "MATCH (b:Block) RETURN b order by b.height desc limit 1;").value()
        if results is None or len(results) == 0:
            return None
        return results[0]

    def supplement_missing(self, block, tx_executor):
        height = block["height"]
        with self.driver.session(database="btc") as session:
            if not session.read_transaction(self.block_exists, height):
                logger.error(f"cannot find block {height}")
                os._exit(0)
        # checking txs
        tx_number = len(block["tx"])
        logger.warning(
            f"checking missing tx({tx_number} in total) for block {height}")
        wait([tx_executor.sumbit(self.supplement_missing_tx_task, block, tx)
              for tx in block["tx"]])

    def block_exists(self, t, height):
        results = t.run(
            "MATCH (b:Block {height: $height}) RETURN b limit 1", height=height).value()
        return results is not None and len(results) != 0

    def block_tx_exists(self, t, height, txid):
        results = t.run(
            "MATCH (tx:Transaction {height: $height, txid: $txid}) RETURN tx limit 1", height=height, txid=txid).value()
        return results is not None and len(results) != 0 and results[0] != 0

    def link_block_with_tx(self, t, block, tx):
        t.run("""
        MATCH (b:Block {hash: $hash}), (tx:Transaction {txid: $txid, height: $height})
        WITH b,tx
        CREATE (b)-[:CONTAINS]->(tx)
        """, hash=block["hash"], txid=tx["txid"], height=block["height"])

    def supplement_missing_tx_task(self, block, tx):
        with self.driver.session(database=self.dbname) as session:
            height = block["height"]
            txid = tx["txid"]
            if not session.read_transaction(self.block_tx_exists, height, txid):
                logger.warning(
                    f"txid {txid} on {height} is not found, supplementing...")
                session.write_transaction(self.parse_tx, block, tx)
                session.write_transaction(self.link_block_with_tx, block, tx)
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
            local_height = 0

        if self.config.get("checker") is not None and local_height > 0:
            blocks_co = self.config["checker"].get("blocks", 100)
            if self.config["syncer"].get("txs"):
                logger.warning("the check thread on btc txs is limited at 1")
            logger.warning(
                f'running on check missing mode, blocks thread {blocks_co}')
            safe_height = self.config["checker"].get(
                "safe-height", local_height - 2 * blocks_co if local_height > blocks_co else 0)

            logger.warning(f"checking from {safe_height} to {local_height}")
            start = safe_height + 1
            with ThreadPoolExecutor(blocks_co) as executor, ThreadPoolExecutor(blocks_co) as tx_executor:
                while start < top_block["height"] - blocks_co:
                    ending = min(start+blocks_co, top_block["height"])
                    logger.warning(f"checking from {start} to {ending}")
                    wait([executor.submit(self.supplement_missing, self.get_block_by_height(
                        height), tx_executor) for height in range(start, ending)])
                    start = ending

        if self.config.get("syncer") is not None and local_height < target:
            if self.config["syncer"].get("blocks"):
                logger.warning("the etl thread on btc block is limited at 1")
            blocks_co = self.config["syncer"].get("txs", 100)
            logger.warning(f'running on sync mode, thread {blocks_co}')
            with ThreadPoolExecutor(blocks_co) as executor:
                for height in range(local_height + 1, target):
                    block = self.get_block_by_height(height)
                    logger.warning("processing block(with {} txs): {} -> {}(remote {})".format(
                        len(block["tx"]), height, target, remote_top))
                    # session.write_transaction(parse_block, rpc, block)
                    self.parse_block(block, executor=executor)
                # if top_node is not None:
                #     t.create(Relationship(block_node, "PREV", top_node))
