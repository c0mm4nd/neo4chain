# %%
from hexbytes.main import HexBytes
from neo4j import GraphDatabase, Transaction
from neo4j.io import ClientError
from web3 import Web3
from helpers import hex_to_int
from reward_calculator import get_const_reward, get_uncle_reward
from web3.datastructures import AttributeDict
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
from concurrent.futures import ThreadPoolExecutor, wait
import time
import logging
import os
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

    def get_hash(self, block_or_tx):
        if type(block_or_tx) is str:
            return block_or_tx
        elif type(block_or_tx) is HexBytes:
            return block_or_tx.hex()
        else:
            return self.get_hash(block_or_tx.hash)

    def parse_block_header(self, t, block):
        results = t.run("""
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
            }) return count(b) as c
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
              gasUsed=str(block.gasUsed)).values()
        assert results[0][0] == 1, results

        results = t.run("""
            MATCH 
                (b:Block {number: $number}),
                (addr:Address {address: $miner_addr})
            CREATE p=(b)-[:BLOCK_REWARD {value: $reward}]->(addr)
            return count(p) as c
        """, number=block.number, miner_addr=block.miner, reward=self.get_block_reward(block)).values()
        assert results[0][0] == 1, results

        # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
        # Only one can enter the ledger as a block, and the other does not
        for uncle_idx in range(0, len(block.uncles)):
            uncle_block = self.w3.eth.get_uncle_by_block(
                block.number, uncle_idx)
            
            results = t.run("""
                MATCH 
                    (b:Block {number: $number}),
                    (addr:Address {address: $miner_addr})
                CREATE p=(b)-[:UNCLE_REWARD {value: $reward}]->(addr)
                return count(p) as c
            """, number=block.number, miner_addr=uncle_block.miner, reward=get_uncle_reward(block['number'], hex_to_int(uncle_block['number']))).values()
            assert results[0][0] == 1, results

    def ensure_block_Addresses(self, block):
        with self.driver.session(database=self.dbname) as session:
            self.insert_Address_EOA(block.miner)
            for uncle_idx in range(0, len(block.uncles)):
                uncle_block = self.w3.eth.get_uncle_by_block(
                    block.number, uncle_idx)
                self.insert_Address_EOA(uncle_block['miner'])
            for transaction in block.transactions:
                transaction_hash = self.get_hash(transaction)
                if transaction.to != None:
                    self.insert_Address_EOA(transaction['from'])  # from must be an EOA
                    self.insert_Address_Unknown(transaction['to'])  # to is unknown
                else:
                    self.insert_Address_EOA(transaction['from'])
                    new_contract_address = self.get_new_contract_address(transaction_hash)
                    assert type(new_contract_address) == str and len(
                        new_contract_address) > 0
                    self.insert_Address_Contract(new_contract_address)
                    logger.info('tx {} created a new contract {}'.format(
                        transaction_hash, new_contract_address))
                
                logs = self.w3.eth.get_transaction_receipt(transaction_hash).logs
                for log in logs:
                    transfer = self.token_transfer_service.extract_transfer_from_log(
                        log)
                    if transfer is not None:
                        for addr in (transfer.from_address, transfer.to_address):
                            self.insert_Address_Unknown(addr)

    def parse_block_tx(self, t, block, transaction):
        assert type(transaction) not in (HexBytes, str) # transaction = self.w3.eth.get_transaction(transaction)

        transaction_hash = self.get_hash(transaction)
        
        self.insert_Transaction(t, transaction)

        if transaction.to != None:
            # insert relationships
            results = t.run("""
            MATCH (tx:Transaction {hash: $hash}),
                (from:Address {address: $from}),
                (to:Address {address: $to})
            CREATE p=(from)-[:SEND]->(tx)-[:TO]->(to)
            return count(p)
            """, {
                'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(),
                'from': transaction['from'], 'to': transaction['to']}).values()
            assert results[0][0] == 1
        else:
            new_contract_address = self.get_new_contract_address(transaction_hash)
            assert type(new_contract_address) == str and len(
                new_contract_address) > 0

            results = t.run("""
            MATCH (tx:Transaction {hash: $hash}),
                (from:Address {address: $from})
            CREATE p=(from)-[:SEND]->(tx)-[:CALL_CONTRACT_CREATION]->(new_contract)
            return count(p)
            """, {
                'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), 
                'from': transaction['from'], 
                'new_contract_address': new_contract_address}
            ).values()
            assert results[0][0] == 1

        logs = self.w3.eth.get_transaction_receipt(transaction_hash).logs
        for log in logs:
            transfer = self.token_transfer_service.extract_transfer_from_log(
                log)
            if transfer is not None:
                self.insert_TokenTransfer(t, transfer)

        results = t.run("""
                MATCH 
                    (b:Block {number: $number}),
                    (tx:Transaction {hash: $hash})
                CREATE p=(b)-[:CONTAINS]->(tx)
                return count(p) as c
            """, number=block.number, hash=transaction_hash).values()
        assert results[0][0] == 1, results

    def get_block_reward(self, block):
        number = block["number"]
        const_reward = get_const_reward(number)
        reward = const_reward

        for transaction in block['transactions']:
            assert type(transaction) is AttributeDict
            transaction_hash = self.get_hash(transaction)
            receipt = self.w3.eth.getTransactionReceipt(transaction_hash)
            fee = receipt.gasUsed * transaction.gasPrice
            reward += fee

        return
    
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

    def insert_Address_Contract(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        def get_bytecode(addr):
            bytecode = self.w3.eth.getCode(Web3.toChecksumAddress(addr))
            bytecode = bytecode if type(
                bytecode) is not HexBytes else bytecode.hex()
            return bytecode

        with self.driver.session(database=self.dbname) as session:
            def try_get_Contract(t, addr):
                return t.run("""
                MATCH (a:Address {address: $address})
                OPTIONAL MATCH (c:Address:Contract {address: $address})
                return a, c
                """, address=addr).data()
            
            result = session.read_transaction(try_get_Contract, addr)

            try:
                if len(result) == 0:  # when a = null
                    def write_Contract(t, addr, bytecode):
                        t.run("""
                        MERGE (a:Address:Contract {address: $address, is_erc20: $is_erc20, is_erc721: $is_erc721, bytecode: $bytecode})
                        """, address=addr, is_erc20=self.is_ERC20(
                            bytecode), is_erc721=self.is_ERC721(bytecode), bytecode=bytecode)
                    bytecode = get_bytecode(addr)
                    session.write_transaction(write_Contract, addr, bytecode)

                elif result[0]['c'] == None:  # when c = null
                    def set_Contract(t, addr, bytecode):
                        t.run("""
                        MATCH (a:Address {address: $address})
                        set a :Contract
                        set a.is_erc20=$is_erc20, a.is_erc721=$is_erc721
                        """, address=addr, is_erc20=self.is_ERC20(
                            bytecode), is_erc721=self.is_ERC721(bytecode))
                    bytecode = get_bytecode(addr)
                    session.write_transaction(set_Contract, addr, bytecode)
                    
            except Exception as e:
                logger.error(e)
                os._exit(0)

    def insert_Address_EOA(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        with self.driver.session(database=self.dbname) as session:
            def try_get_EOA(t, addr):
                return t.run("""
                MATCH (a:Address {address: $address})
                OPTIONAL MATCH (c:Address:EOA {address: $address})
                return a, c
                """, address=addr).data()
            result = session.read_transaction(try_get_EOA, addr)

            try:
                if len(result) == 0:
                    def write_EOA(t, addr):
                        t.run("""
                        MERGE (a:Address:EOA {address: $address})
                        """, address=addr)
                    session.write_transaction(write_EOA, addr)
                elif result[0].get('eoa') == None:  # when eoa = null
                    def set_EOA(t, addr):
                        t.run("""
                        MATCH (a:Address {address: $address})
                        SET a :EOA
                        """, address=addr)
                    session.write_transaction(set_EOA, addr)
            except Exception as e:
                logger.error(e)
                os._exit(0)

    def insert_Address_Unknown(self, addr):
        # https://stackoverflow.com/questions/21625081/add-label-to-existing-node-with-cypher
        if type(addr) is HexBytes:
            addr = addr.hex()
        with self.driver.session(database=self.dbname) as session:
            def try_get_Addr(t, addr):
                return t.run("""
                MATCH (a:Address {address: $address})
                return a
                """, address=addr)
            result = session.read_transaction(try_get_Addr, addr)

            if len(result.values()) == 0:
                try:
                    self.insert_Address_EOA(addr)
                # logger.warning("address {} doesnt exist".format(addr))
                except Exception as e:
                    logger.error(e)
                    os._exit(0)

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
        results = t.run("""
        CREATE (a:TokenTransfer {
            hash_idx: $hash_idx,
            token_address: $token_addr,         
            value: $value,
            value_raw: $value_raw
        })
        return count(a)
        """, hash_idx=hash_idx,
              token_addr=transfer.token_address,  # do not add (Contract)-[handles]->[TokenTransfer] to avoid 1-INF too heavy relationship
              value=str(transfer.value),
              value_raw=transfer.value_raw
              ).values()
        assert results[0][0] == 1

        # add from replationships & add to replationships
        results = t.run("""
            MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
                (from:Address {address: $from}),
                (to:Address {address: $to})
            CREATE (from)-[:SEND_TOKEN]->(tf)-[:TOKEN_TO]->(to)
            """, {'hash_idx': hash_idx, 'from': transfer.from_address, 'to': transfer.to_address}).values()
        assert results[0][0] == 1
        # add tx_hash replationships
        results = t.run("""
            MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
                (tx:Transaction {hash: $hash})
            CREATE p=(tx)-[:CALL_TOKEN_TRANSFER]->(tf)
            return count(p)
            """, hash_idx=hash_idx, hash=transfer.transaction_hash).values()
        assert results[0][0] == 1


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

    def get_local_Block(self, t, number):
        results = t.run(
            "MATCH (b:Block {number: $number}) RETURN b;", number=number).data()
        if type(results) is not list:
            logger.error(
                f"failed to inspect Block @ {number}: results are {results}")
            os._exit(0)
        if len(results) != 1 or results[0] is None:
            return None
        return results[0]['b']

    def get_local_Transaction(self, t, hash):
        results = t.run(
            "MATCH (tx:Transaction {hash: $hash}) RETURN tx;", hash=hash).data()
        if type(results) is not list:
            logger.error(
                f"failed to inspect Transaction as {hash}: results are {results}")
            os._exit(0)
        if len(results) != 1 or results[0] is None:
            return None
        return results[0]['tx']

    def check_task(self, number):
        logger.info("checking block {}".format(number))
        
        block = self.w3.eth.get_block(number, full_transactions=True)
        self.ensure_block_Addresses(block) # should not in any session context
        
        with self.driver.session(database=self.dbname) as session:
            local_block = session.read_transaction(
                self.get_local_Block, number)
            
            if local_block is None:
                logger.warning(f'Missing block {number}')
                session.write_transaction(self.parse_block_header, block)
                logger.warning(f"supplemented block {number}")

                for transaction in block.transactions:
                    transaction_hash = self.get_hash(transaction)
                    if session.read_transaction(self.get_local_Transaction, transaction_hash) is None:
                        logger.warning(f'missing transaction {transaction_hash}')
                        session.write_transaction(
                            self.parse_block_tx, block, transaction)
                        logger.warning(f"supplemented tx {transaction_hash}")
            else:
                logger.info(f'Block {number} exists, checking its txs')
                for transaction in block.transactions:
                    transaction_hash = self.get_hash(transaction)
                    if session.read_transaction(self.get_local_Transaction, transaction_hash) is None:
                        logger.warning(f'missing transaction {transaction_hash}')
                        session.write_transaction(
                            self.parse_block_tx, block, transaction)
                        logger.warning(f"supplemented tx {transaction_hash}")

            return

    def check_missing(self, local_height, co=100, safe_height=0):
        logger.warning(
            f'check missing blocks from {safe_height} to {local_height} with max {co} threads')

        height = safe_height
        # run multi thread in block level
        with ThreadPoolExecutor(max_workers=co) as executor:
            while height < local_height:
                next_height = height + co
                if next_height > local_height:
                    next_height = local_height
                wait([executor.submit(self.check_task, height)
                     for height in range(height, next_height)])
                height = next_height

    def work_flow(self):
        latest = self.w3.eth.get_block(
            'latest', full_transactions=False).number
        local_height = self.get_local_block_height()
        logger.warning(f'local height {local_height}, remote {latest}')
        if self.config.get("checker") is not None and local_height > 0:
            co = self.config["checker"].get("thread", 1000)
            logger.warning(f'running on check missing mode, thread {co}')
            safe_height = self.config["checker"].get("safe-height")
            if safe_height is None or safe_height < 0:
                safe_height = local_height - co * 10 if local_height > 1000 else 0

            if co is not None:
                self.check_missing(local_height, co=co,
                                   safe_height=safe_height)
            else:
                self.check_missing(local_height, safe_height=safe_height)

        if self.config.get("syncer") is not None and local_height < latest - 1000:
            co = self.config["syncer"].get("thread", 100)
            logger.warning(f'running on slow sync mode, thread {co}.')
            logger.warning(
                'suggest export csv and manually import with neo4j-admin')

            def threadsafe_parse_block_tx(block, transaction):
                with self.driver.session(database=self.dbname) as session:
                    logger.info("parsing tx {}".format(self.get_hash(transaction)))
                    session.write_transaction(
                        self.parse_block_tx, block, transaction)

            with ThreadPoolExecutor(co) as executor:
                # run multi thread in txs
                for number in range(local_height + 1, latest):
                    block = self.w3.eth.get_block(
                        number, full_transactions=True)
                    logger.warning(
                        'processing block(with {} txs) {} -> {}'.format(
                            len(block.transactions), block.number, latest
                        ))
                    
                    self.ensure_block_Addresses(block)
                    with self.driver.session(database=self.dbname) as session:
                        session.write_transaction(self.parse_block_header, block)
                    logger.info("start parsing txs")
                    wait([executor.submit(threadsafe_parse_block_tx, block, transaction)
                          for transaction in block.transactions])

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
                            session.write_transaction(
                                self.parse_block_header, block)

                        wait([executor.submit(threadsafe_parse_block_tx, block, transaction)
                              for transaction in block.transactions])

                    time.sleep(60*60*24)  # sleep one day
