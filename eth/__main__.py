# %%
import json
from hexbytes.main import HexBytes
from neo4j import GraphDatabase, Session
from neo4j.io import ClientError
from web3 import Web3
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
from threading import Thread
import time
import logging
import os
import argparse
import requests

logger = logging.getLogger(__name__)


class EthereumETL:
    contract_service = EthContractService()
    token_transfer_service = EthTokenTransferExtractor()

    def __init__(self, config):
        self.config = config
        rpc_config = config["daemon"]
        neo4j_config = config["neo4j"]

        # Websocket is not supported under multi thread
        # https://github.com/ethereum/web3.py/issues/2090
        # w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8546'))
        # w3 = Web3(Web3.WebsocketProvider(
        #     'wss://mainnet.infura.io/ws/v3/dc6980e1063b421bbcfef8d7f58ccd43'))
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=2**16, pool_maxsize=2**16)
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.w3 = Web3(Web3.HTTPProvider(rpc_config["address"],
                                         session=session, request_kwargs={'timeout': 20}))
        logger.warning('using web3@'+self.w3.api)

        self.driver = GraphDatabase.driver(
            neo4j_config["address"], auth=(neo4j_config["username"], neo4j_config["password"]))

        self.dbname = neo4j_config.get("database", "eth")

        self.ensure_db_exists()

    def drop_db(self):
        system = self.driver.session()
        system.run(f"DROP DATABASE {self.dbname}")

    def create_db(self):
        system = self.driver.session()
        system.run(f"CREATE DATABASE {self.dbname}")
        system.close()

        with self.driver.session(database=self.dbname) as session:
            session.run(
                "CREATE CONSTRAINT block_hash_uq ON (block:Block) ASSERT block.hash IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT block_number_uq ON (block:Block) ASSERT block.number IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT addr_uq ON (addr:Address) ASSERT addr.address IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT tx_hash_uq ON (tx:Transaction) ASSERT tx.hash IS UNIQUE")
            session.run(
                "CREATE CONSTRAINT tf_hash_idx_uq ON (tx:TokenTransfer) ASSERT tx.hash_idx IS UNIQUE")

    def ensure_db_exists(self):
        with self.driver.session(database=self.dbname) as session:
            try:
                session.run("create (placeholder:Block {height: -1})")
                session.run(
                    "MATCH  (placeholder:Block {height: -1}) delete placeholder")
            except ClientError as e:
                if e.code == 'Neo.ClientError.Database.DatabaseNotFound':
                    self.create_db()
                else:
                    raise e

    def parse_block(self, t, block):
        t.run("""
            create (b:Block {
                number: $number, 
                hash: $hash,
                timestamp: $timestamp,
                size: $size,
                nonce: $nonce,
                difficulty: $difficulty,
                totalDifficulty: $totalDifficulty,
                gasLimit: $gasLimit,
                gasUsed: $gasUsed
            }) return b
            """,
              number=block.number,
              hash=block.hash if type(
                  block.hash) is not HexBytes else block.hash.hex(),
              timestamp=block.timestamp,
              size=block.size,
              nonce=block.nonce if type(
                  block.nonce) is not HexBytes else block.nonce.hex(),
              difficulty=str(block.difficulty),
              totalDifficulty=str(block.totalDifficulty),
              gasLimit=str(block.gasLimit),
              gasUsed=str(block.gasUsed))

        # miner must be an EOA
        self.insert_EOA(t, block.miner)

        # todo: add reward amount into the reward relationships
        t.run("""
            MATCH 
                (b:Block {number: $number}),
                (addr:Address {address: $miner_addr})
            CREATE (b)-[:BLOCK_REWARD]->(addr)
        """, number=block.number, miner_addr=block.miner)  # TODO: BlockReward value

        # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
        # Only one can enter the ledger as a block, and the other does not
        for uncle_idx in range(0, len(block.uncles)):
            uncle_block = self.w3.eth.get_uncle_by_block(
                block.number, uncle_idx)
            t.run("""
                MATCH 
                    (b:Block {number: $number}),
                    (addr:Address {address: $miner_addr})
                CREATE (b)-[:UNCLE_REWARD]->(addr)
            """, number=block.number, miner_addr=uncle_block.miner)  # TODO: UncleReward value

        for transaction in block.transactions:
            if type(transaction) in (HexBytes, str):
                transaction = self.w3.eth.get_transaction(transaction)
            self.parse_Transaction(t, transaction)

        for transaction in block.transactions:
            if type(transaction) in (HexBytes, str):
                transaction_hash = transaction
            else:
                transaction_hash = transaction.hash
            self.parse_TokenTransfer(t, transaction_hash)

        for transaction in block.transactions:
            if type(transaction) in (HexBytes, str):
                transaction_hash = transaction
            else:
                transaction_hash = transaction.hash
            t.run("""
                MATCH 
                    (b:Block {number: $number}),
                    (tx:Transaction {hash: $hash})
                CREATE (b)-[:CONTAINS]->(tx)
            """, number=block.number, hash=transaction_hash)

    def parse_Transaction(self, t, transaction):
        self.insert_Transaction(t, transaction)

        if transaction.to != None:
            self.insert_Address(t, transaction['to'])  # to is unknown
            self.insert_EOA(t, transaction['from'])  # from must be an EOA
            # insert relationships
            t.run("""
            MATCH (tx:Transaction {hash: $hash}),
                (from:Address {address: $from}),
                (to:Address {address: $to})
            CREATE (from)-[:SEND]->(tx)-[:TO]->(to)
            """, {'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), 'from': transaction['from'], 'to': transaction['to']})
        else:
            self.insert_EOA(t, transaction['from'])
            new_contract_address = self.get_new_contract_address(transaction.hash if type(
                transaction.hash) is not HexBytes else transaction.hash.hex())
            assert type(new_contract_address) == str and len(
                new_contract_address) > 0
            self.insert_Contract(t, new_contract_address)
            logger.info('tx {} created a new contract {}'.format(
                transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), new_contract_address))

            t.run("""
            MATCH (tx:Transaction {hash: $hash}),
                (from:Address {address: $from})
            CREATE (from)-[:SEND]->(tx)-[:CALL_CONTRACT_CREATION]->(new_contract)
            """, {'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), 'from': transaction['from'], 'new_contract_address': new_contract_address})

    def get_new_contract_address(self, transaction_hash):
        receipt = self.w3.eth.getTransactionReceipt(transaction_hash)
        return receipt.contractAddress  # 0xabcd in str

    def is_ERC20(self, bytecode):
        # contains bug here
        # https://github.com/blockchain-etl/ethereum-etl/issues/194
        # https://github.com/blockchain-etl/ethereum-etl/issues/195
        function_sighashes = self.contract_service.get_function_sighashes(
            bytecode)
        return self.contract_service.is_erc20_contract(function_sighashes)

    def is_ERC721(self, bytecode):
        function_sighashes = self.contract_service.get_function_sighashes(
            bytecode)
        return self.contract_service.is_erc721_contract(function_sighashes)

    def insert_Contract(self, tx, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        query = """
        MERGE (a:Address {address: $address})
        set a.type = 2, a.is_erc20=$is_erc20, a.is_erc721=$is_erc721
        """
        bytecode = self.w3.eth.getCode(Web3.toChecksumAddress(addr))
        bytecode = bytecode if type(
            bytecode) is not HexBytes else bytecode.hex()

        tx.run(query, address=addr, is_erc20=self.is_ERC20(
            bytecode), is_erc721=self.is_ERC721(bytecode))

    def insert_EOA(self, t, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        t.run("""
        MERGE (a:Address {address: $address})
        set a.type = 1
        """, address=addr)

    def insert_Address(self, t, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        query = "MERGE (a:Address {address: $address})"
        t.run(query, address=addr)

    def insert_Transaction(self, t, transaction):
        if type(transaction['transactionIndex']) is str and transaction['transactionIndex'].startswith('0x'):
            transaction['transactionIndex'] = int(
                transaction['transactionIndex'][2:], 16)

        t.run("""
        CREATE (tx:Transaction {
            hash: $hash,
            from: $from,
            to: $to,
            value: $value,
            input: $input,
            nonce: $nonce,
            r: $r,
            s: $s,
            v: $v,
            transactionIndex: $transactionIndex,
            gas: $gas,
            gasPrice: $gasPrice
        }) 
        """, {
            'hash':  transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(),
            'from': transaction['from'],
            'to': transaction['to'],
            'value': str(transaction['value']),
            'input': transaction['input'],
            'nonce': transaction['nonce'],
            'r': transaction['r'] if type(transaction['r']) is not HexBytes else transaction['r'].hex(),
            's': transaction['s'] if type(transaction['s']) is not HexBytes else transaction['s'].hex(),
            'v': transaction['v'],
            'transactionIndex': transaction['transactionIndex'],
            # 'type': transaction['type'], cannot get type from openethereum, and not officially supported https://eth.wiki/json-rpc/API
            'gas': str(transaction['gas']),
            'gasPrice': str(transaction['gasPrice'])})

    def insert_TokenTransfer(self, t, transfer):
        # define hash_idx
        hash_idx = transfer.transaction_hash + '.' + str(transfer.log_index)

        # transfer struct
        # https://github.com/blockchain-etl/ethereum-etl/blob/develop/ethereumetl/domain/token_transfer.py#L24
        t.run("""
        MERGE (a:TokenTransfer {
            hash_idx: $hash_idx,
            token_address: $token_addr,         
            value: $value
        })
        """, hash_idx=hash_idx,
              token_addr=transfer.token_address,  # do not add (Contract)-[handles]->[TokenTransfer] to avoid 1-INF too heavy relationship
              value=str(transfer.value))

        for addr in (transfer.from_address, transfer.to_address):
            self.insert_Address(t, addr)

        # add from replationships & add to replationships
        t.run("""
            MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
                (from:Address {address: $from}),
                (to:Address {address: $to})
            CREATE (from)-[:SEND_TOKEN]->(tf)-[:TOKEN_TO]->(to)
            """, {'hash_idx': hash_idx, 'from': transfer.from_address, 'to': transfer.to_address})
        # add tx_hash replationships
        t.run("""
            MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
                (tx:Transaction {hash: $hash})
            CREATE (tx)-[:CALL_TOKEN_TRANSFER]->(tf)
            """, hash_idx=hash_idx, hash=transfer.transaction_hash)

    def parse_TokenTransfer(self, t, transaction_hash):
        # load token transfer from receipt logs
        logs = self.w3.eth.getTransactionReceipt(transaction_hash).logs
        for log in logs:
            transfer = self.token_transfer_service.extract_transfer_from_log(
                log)
            if transfer is not None:
                self.insert_TokenTransfer(t, transfer)

    def get_local_block_height(self):
        with self.driver.session(database=self.dbname) as session:
            results = session.run(
                "MATCH (b:Block) RETURN max(b.number);").value()
            if results[0] is None:
                return -1
            else:
                return results[0]

    def get_local_block_timestamp(self):
        with self.driver.session(database=self.dbname) as session:
            results = session.run(
                "MATCH (b:Block) with max(b.number) as top match (b:Block) where b.number = top return b.timestamp;").value()
            if results[0] is None:
                return -1
            else:
                return results[0]

    def thread_task(self, height, latest):
        with self.driver.session(database=self.dbname) as session:
            retry = 0
            while True:
                try:
                    block = self.w3.eth.get_block(height, full_transactions=True)
                    break
                except Exception as e:
                    logger.error("failed to fetch block on syncing")
                    logger.error(e)
                    if retry == 3:
                        os._exit(0)
                retry += 1

            retry = 0
            while True:
                try:
                    logger.warning('processing block(with {} txs) {} -> {}'.format(
                        len(block.transactions), block.number, latest
                    ))
                    session.write_transaction(self.parse_block, block)
                    break
                except Exception as e:
                    logger.error("failed to parse block on syncing")
                    logger.error(e)
                    if retry == 3:
                        os._exit(0)
                retry += 1

    def block_exists(self, t, height):
        results = t.run(
            "MATCH (b:Block {number: $height}) RETURN b.number;", height=height).value()
        if type(results) is not list:
            logger.error(
                f"failed to inspect Block on {height}: results are {results}")
            os._exit(0)
        if len(results) != 1 or results[0] is None:
            return False
        return True

    def check_task(self, height):
        with self.driver.session(database=self.dbname) as session:
            if session.read_transaction(self.block_exists, height):
                # logger.warning(f'Block {height} exists')
                pass
            else:
                logger.warning(f'Missing block {height}')
                while True:
                    retry = 0
                    try:
                        block = self.w3.eth.get_block(
                            height, full_transactions=True)
                        break
                    except Exception as e:
                        logger.error(f'fetching {height} failed')
                        logger.error(e)
                        if retry == 3:
                            os._exit(0)
                    time.sleep(2)
                    retry += 1
                while True:
                    retry = 0
                    try:
                        session.write_transaction(self.parse_block, block)
                        logger.warning(f"supplemented block {height}")
                        return
                    except Exception as e:
                        logger.error(f'parsing {height} failed')
                        logger.error(e)
                        if retry == 3:
                            os._exit(0)
                    time.sleep(2)
                    retry += 1


    def check_missing(self, local_height, co=100, safe_height=0):
        logger.warning(
            f'check missing blocks from {safe_height} to {local_height}')

        height = safe_height
        while height < local_height:
            next_height = height + co
            if next_height > local_height:
                next_height = local_height
            tasks = [Thread(target=self.check_task, args=(i,))
                     for i in range(height, next_height)]
            for t in tasks:
                t.start()
            for t in tasks:
                t.join()
            height = next_height

    def work_flow(self):
        latest = self.w3.eth.get_block(
            'latest', full_transactions=False).number
        local_height = self.get_local_block_height()
        if self.config.get("checker") is not None and local_height > 0:
            co = self.config["checker"].get("thread", 1000)
            logger.warning(f'running on check missing mode, thread {co}')
            safe_height = self.config["checker"].get("safe-height")
            if safe_height is None or safe_height < 0:
                safe_height = local_height - 1000 if local_height > 1000 else 0
            
            if co is not None:
                self.check_missing(local_height, co=co,
                                   safe_height=safe_height)
            else:
                self.check_missing(local_height, safe_height=safe_height)

        if self.config.get("syncer") is not None and local_height < latest - 1000:
            co = self.config["syncer"].get("thread", 100)
            logger.warning(f'running on fast sync mode, thread {co}')

            while local_height < latest-1000:
                tasks = [Thread(target=self.thread_task, args=(
                    local_height+i+1, latest)) for i in range(co)]

                # start all
                for t in tasks:
                    t.start()
                for t in tasks:
                    t.join()
                local_height += co

            logger.warning("entering daily sync mode")
            while True:
                latest = self.w3.eth.get_block(
                    'latest', full_transactions=False).number
                local_timestamp = self.get_local_block_timestamp()
                while True:
                    local_height += 1
                    block = self.w3.eth.getBlock(
                        local_height, full_transactions=True)
                    if block.timestamp - local_timestamp < 60*60*24:
                        break
                    logger.warning('processing block(with {} txs) {} -> {}'.format(
                        len(block.transactions), local_height, latest
                    ))
                    with self.driver.session(database=self.dbname) as session:
                        session.write_transaction(self.parse_block, block)

                time.sleep(60*60*24)  # sleep one day


# %%


if __name__ == '__main__':
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
        config_content = json.load(file)
    eth_config = config_content["eth"]

    etl = EthereumETL(eth_config)
    etl.work_flow()
