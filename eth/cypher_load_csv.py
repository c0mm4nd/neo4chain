# %%
import json
from pathlib import Path
import threading
from hexbytes.main import HexBytes
from neo4j import GraphDatabase, Session, Transaction
from neo4j.io import ClientError
from web3 import Web3
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, wait
import time
import logging
import os
import argparse
import requests
import csv

logger = logging.getLogger(__name__)

queries = ["""
                load csv with headers from 'file:///ethereum_etl_address.csv' as line
                with line
                MERGE (a:Address {address: line.address})
                set a.type = toInteger(line.type), a.is_erc20=toBooleanOrNull(line.is_erc20), a.is_erc721=toBooleanOrNull(line.is_erc721)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_block.csv' as line
                with line
                create (b:Block {
                    number: toInteger(line.number),
                    hash: line.hash,
                    timestamp: toInteger(line.timestamp),
                    size: toInteger(line.size),
                    nonce: line.nonce,
                    difficulty: line.difficulty,
                    totalDifficulty: line.totalDifficulty,
                    gasLimit: line.gasLimit,
                    gasUsed: line.gasUsed
                }) return count(b)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_transaction.csv' as line
                with line
                CREATE (tx:Transaction {
                    hash: line.hash,
                    from: line.from,
                    to: line.to,
                    value: line.value,
                    input: line.input,
                    nonce: line.nonce,
                    r: line.r,
                    s: line.s,
                    v: line.v,
                    transactionIndex: toInteger(line.transactionIndex),
                    gas: line.gas,
                    gasPrice: line.gasPrice
                }) return count(tx)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_transfer.csv' as line
                with line
                CREATE (a:TokenTransfer {
                    hash_idx: line.hash_idx,
                    token_address: line.token_addr,
                    value: line.value
                })
                """,
           ############################ REL ############################
           """
                load csv with headers from 'file:///ethereum_etl_block_reward.csv' as line
                with line
                MATCH
                    (b:Block {number: toInteger(line.number)}),
                    (addr:Address {address: line.miner_addr})
                CREATE (b)-[:BLOCK_REWARD]->(addr)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_uncle_reward.csv' as line
                with line
                MATCH
                    (b:Block {number: toInteger(line.number)}),
                    (addr:Address {address: line.miner_addr})
                CREATE (b)-[:UNCLE_REWARD]->(addr)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_contains.csv' as line
                MATCH
                    (b:Block {number: toInteger(line.number)}),
                    (tx:Transaction {hash: line.hash})
                CREATE (b)-[:CONTAINS]->(tx)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_send.csv' as line
                MATCH (tx:Transaction {hash: line.hash}),
                    (from:Address {address: line.from})
                CREATE (from)-[:SEND]->(tx)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_to.csv' as line
                MATCH (tx:Transaction {hash: line.hash}),
                    (to:Address {address: line.to})
                CREATE (tx)-[:TO]->(to)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_call_contract_creation.csv' as line
                MATCH (tx:Transaction {hash: line.hash}),
                    (new_contract:Address {address: line.new_contract_address})
                CREATE (tx)-[:CALL_CONTRACT_CREATION]->(new_contract)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_contains.csv' as line
                MATCH
                    (b:Block {number: toInteger(line.number)}),
                    (tx:Transaction {hash: line.hash})
                CREATE (b)-[:CONTAINS]->(tx)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_send_token.csv' as line
                MATCH (tf:TokenTransfer {hash_idx: line.hash_idx}),
                    (from:Address {address: line.from}),
                    (to:Address {address: line.to})
                CREATE (from)-[:SEND_TOKEN]->(tf)-[:TOKEN_TO]->(to)
                """,
           """
                load csv with headers from 'file:///ethereum_etl_call_token_transfer.csv' as line
                MATCH (tf:TokenTransfer {hash_idx: line.hash_idx}),
                    (tx:Transaction {hash: line.hash})
                CREATE (tx)-[:CALL_TOKEN_TRANSFER]->(tf)
                """]


class DictWriter(csv.DictWriter):
    def __init__(self, f, fieldnames, restval="", extrasaction="raise",
                 dialect="excel", *args, **kwds):
        self._lock = threading.Lock()
        super().__init__(f, fieldnames, restval, extrasaction,
                         dialect, *args, **kwds)

    def writerow(self, rowdict):
        with self._lock:
            super().writerow(rowdict)


class EthereumCSVETL:
    contract_service = EthContractService()
    token_transfer_service = EthTokenTransferExtractor()

    def __init__(self, config):
        self.config = config
        rpc_config = config["daemon"]
        neo4j_config = config["neo4j"]

        self.csv = {}
        self.writter = {}
        self.queries = queries

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

    def parse_block(self, block):
        self.writter['block'].writerow({
            'number': block.number,
            'hash': block.hash if type(
                block.hash) is not HexBytes else block.hash.hex(),
            'timestamp': block.timestamp,
            'size': block.size,
            'nonce': block.nonce if type(
                block.nonce) is not HexBytes else block.nonce.hex(),
            'difficulty': str(block.difficulty),
            'totalDifficulty': str(block.totalDifficulty),
            'gasLimit': str(block.gasLimit),
            'gasUsed': str(block.gasUsed)})

        # miner must be an EOA
        self.insert_EOA(block.miner)

        # todo: add reward amount into the reward relationships
        self.writter['block_reward'].writerow(
            {'number': block.number, 'miner_addr': block.miner})  # TODO: BlockReward value

        # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
        # Only one can enter the ledger as a block, and the other does not
        for uncle_idx in range(0, len(block.uncles)):
            uncle_block = self.w3.eth.get_uncle_by_block(
                block.number, uncle_idx)
            self.writter['uncle_reward'].writerow(
                {'number': block.number, 'miner_addr': uncle_block.miner})  # TODO: UncleReward value

        for transaction in block.transactions:
            if type(transaction) in (HexBytes, str):
                transaction = self.w3.eth.get_transaction(transaction)
            self.parse_Transaction(transaction)

        for transaction in block.transactions:
            if type(transaction) in (HexBytes, str):
                transaction_hash = transaction
            else:
                transaction_hash = transaction.hash
            self.parse_TokenTransfer(transaction_hash)

        for transaction in block.transactions:
            if type(transaction) in (HexBytes, str):
                transaction_hash = transaction
            else:
                transaction_hash = transaction.hash
            self.writter['contains'].writerow(
                {'number': block.number, 'hash': transaction_hash})

    def parse_Transaction(self, transaction):
        self.insert_Transaction(transaction)

        if transaction.to != None:
            self.insert_Address(transaction['to'])  # to is unknown
            self.insert_EOA(transaction['from'])  # from must be an EOA
            # insert relationships
            self.writter['send'].writerow({'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(),
                                           'from': transaction['from']})
            self.writter['to'].writerow({'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(),
                                         'to': transaction['to']})
        else:
            self.insert_EOA(transaction['from'])
            new_contract_address = self.get_new_contract_address(transaction.hash if type(
                transaction.hash) is not HexBytes else transaction.hash.hex())
            assert type(new_contract_address) == str and len(
                new_contract_address) > 0
            self.insert_Contract(new_contract_address)
            logger.info('tx {} created a new contract {}'.format(
                transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), new_contract_address))
            self.writter['send'].writerow({'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(),
                                           'from': transaction['from']})
            self.writter['call_contract_creation'].writerow({'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(),
                                                             'new_contract_address': new_contract_address})

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

    def insert_Contract(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        bytecode = self.w3.eth.getCode(Web3.toChecksumAddress(addr))
        bytecode = bytecode if type(
            bytecode) is not HexBytes else bytecode.hex()
        assert addr is not None
        self.writter['address'].writerow({'type': 2, 'address': addr, 'is_erc20': self.is_ERC20(
            bytecode), 'is_erc721': self.is_ERC721(bytecode)})

    def insert_EOA(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        assert addr is not None
        self.writter['address'].writerow({'type': 1, 'address': addr})

    def insert_Address(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        assert addr is not None
        self.writter['address'].writerow({'type': None, 'address': addr})

    def insert_Transaction(self, transaction):
        if type(transaction['transactionIndex']) is str and transaction['transactionIndex'].startswith('0x'):
            transaction['transactionIndex'] = int(
                transaction['transactionIndex'][2:], 16)

        self.writter['transaction'].writerow({
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

    def insert_TokenTransfer(self, transfer):
        # define hash_idx
        hash_idx = transfer.transaction_hash + '.' + str(transfer.log_index)

        # transfer struct
        # https://github.com/blockchain-etl/ethereum-etl/blob/develop/ethereumetl/domain/token_transfer.py#L24
        self.writter['transfer'].writerow({'hash_idx': hash_idx,
                                           # do not add (Contract)-[handles]->[TokenTransfer] to avoid 1-INF too heavy relationship
                                           'token_addr': transfer.token_address,
                                           'value': str(transfer.value)})

        for addr in (transfer.from_address, transfer.to_address):
            self.insert_Address(addr)

        # add from replationships & add to replationships
        self.writter['send_token'].writerow(
            {'hash_idx': hash_idx, 'from': transfer.from_address, 'to': transfer.to_address})
        # add tx_hash replationships
        self.writter['call_token_transfer'].writerow(
            {'hash_idx': hash_idx, 'hash': transfer.transaction_hash})

    def parse_TokenTransfer(self, transaction_hash):
        # load token transfer from receipt logs
        logs = self.w3.eth.getTransactionReceipt(transaction_hash).logs
        for log in logs:
            transfer = self.token_transfer_service.extract_transfer_from_log(
                log)
            if transfer is not None:
                self.insert_TokenTransfer(transfer)

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
                    block = self.w3.eth.get_block(
                        height, full_transactions=True)
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
                    self.parse_block(block)
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

    def close_csv(self):
        for key in self.csv:
            self.csv[key].close()

    def load_csv(self, t):
        for query in queries:
            t.run(query)

    def save_csv(self):
        for key in self.csv:
            tmp_file = self.csv[key]
            filename = os.path.join(
                self.config["syncer"].get("import-folder", "import"),
                Path(tmp_file.name).stem,
            )
            with open(tmp_file.name, 'r') as tmp, open(filename, 'a') as f:
                f.writelines(tmp.readlines())

    def save_height(self, height):
        filename = os.path.join(
            self.config["syncer"].get("import-folder", "import"),
            "height"
        )
        with open(filename, "w") as f:
            f.write(str(height))

    def init_csv(self):
        self.csv['address'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_address.csv'), 'w')
        self.csv['block'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_block.csv'), 'w')
        self.csv['transaction'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_transaction.csv'), 'w')
        self.csv['transfer'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_transfer.csv'), 'w')
        self.csv['block_reward'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_block_reward.csv'), 'w')
        self.csv['uncle_reward'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_uncle_reward.csv'), 'w')
        self.csv['send'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_send.csv'), 'w')
        self.csv['to'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_to.csv'), 'w')
        self.csv['call_contract_creation'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_call_contract_creation.csv'), 'w')
        self.csv['contains'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_contains.csv'), 'w')
        self.csv['send_token'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_send_token.csv'), 'w')
        self.csv['call_token_transfer'] = open(os.path.join(
            self.config["syncer"].get("tmp-folder", "tmp"), 'ethereum_etl_call_token_transfer.csv'), 'w')

        self.writter['address'] = DictWriter(self.csv['address'], fieldnames=[
            'address', 'type', 'is_erc20', 'is_erc721'])
        self.writter['block'] = DictWriter(self.csv['block'], fieldnames=[
            'number', 'hash', 'timestamp', 'size', 'nonce', 'difficulty', 'totalDifficulty', 'gasLimit', 'gasUsed'
        ])
        self.writter['transaction'] = DictWriter(self.csv['transaction'], fieldnames=[
            'hash', 'from', 'to', 'value', 'input', 'nonce', 'r', 's', 'v', 'transactionIndex', 'gas', 'gasPrice'])
        self.writter['transfer'] = DictWriter(self.csv['transfer'], fieldnames=[
            'hash_idx', 'token_addr', 'value'])
        self.writter['block_reward'] = DictWriter(self.csv['block_reward'], fieldnames=[
            'number', 'miner_addr'])
        self.writter['uncle_reward'] = DictWriter(self.csv['uncle_reward'], fieldnames=[
            'number', 'miner_addr'])
        self.writter['send'] = DictWriter(self.csv['send'], fieldnames=[
            'hash', 'from'])
        self.writter['to'] = DictWriter(self.csv['to'], fieldnames=[
            'hash', 'to'])
        self.writter['call_contract_creation'] = DictWriter(self.csv['call_contract_creation'], fieldnames=[
            'hash', 'new_contract_address'])
        self.writter['contains'] = DictWriter(self.csv['contains'], fieldnames=[
            'number', 'hash'])
        self.writter['send_token'] = DictWriter(self.csv['send_token'], fieldnames=[
            'hash_idx', 'from', 'to'])
        self.writter['call_token_transfer'] = DictWriter(self.csv['call_token_transfer'], fieldnames=[
            'hash_idx', 'hash'])
        for key in self.writter:
            self.writter[key].writeheader()

    def work_flow(self):
        local_height = self.get_local_block_height()
        logger.warning(f"the height in neo4j is {local_height}")
        latest = self.w3.eth.get_block(
            'latest', full_transactions=False).number

        #
        # WARNNING: no integrity check
        #

        assert(self.config.get("syncer") is not None,
               'eth.syncer object is required in config')

        if local_height < 0 and self.config["syncer"].get("manually-height") is not None:
            local_height = self.config["syncer"]["manually-height"]
            logger.warning(f"local height is set to {local_height}")
        co = self.config["syncer"].get("thread", 100)
        logger.warning(f'running on fast sync mode, thread {co}')
        # self.config["syncer"].get("tmp-folder", "tmp")

        with ThreadPoolExecutor(max_workers=min(co, 256)) as executor:
            while local_height < latest-1000:
                self.init_csv()

                tasks = [executor.submit(
                    self.thread_task, local_height+i+1, latest) for i in range(co)]
                wait(tasks)

                self.close_csv()
                self.save_csv()

                if not self.config["syncer"].get("manually-import", False):
                    with self.driver.session(database=self.dbname) as session:
                        session.write_transaction(self.load_csv)

                local_height += co  # self.get_local_block_height()
                self.save_height(local_height)
                logger.warning(f'height updated to {local_height}')


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

    etl = EthereumCSVETL(eth_config)
    etl.work_flow()
