# %% script for extracting data for neo4j admin import (initial)
import json
from pathlib import Path
import threading
from hexbytes.main import HexBytes
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

class TSDictWriter(csv.DictWriter):
    def __init__(self, f, fieldnames, restval="", extrasaction="raise",
                 dialect="excel", *args, **kwds):
        self._lock = threading.Lock()
        super().__init__(f, fieldnames, restval, extrasaction,
                         dialect, *args, **kwds)

    def writerow(self, rowdict):
        with self._lock:
            super().writerow(rowdict)

contract_service = EthContractService()
token_transfer_service = EthTokenTransferExtractor()

class EthereumCSVETL:
    def __init__(self, config):
        self.config = config
        rpc_config = config["daemon"]
        neo4j_config = config["neo4j"]

        self.csv = {}
        self.writter = {}

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=2**16, pool_maxsize=2**16)
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.w3 = Web3(Web3.HTTPProvider(rpc_config["address"],
                                         session=session, request_kwargs={'timeout': 20}))
        logger.warning('using web3@'+self.w3.api)

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
                                           'value': str(transfer.value),
                                           'value_raw': transfer.value_raw,
                                           })

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

    def load_height(self):
        filename = os.path.join(
            self.config["syncer"].get("import-folder", "import"),
            "height"
        )
        try:
            with open(filename) as f:
                content = f.readline()
                return int(content)
        except:
            return -1


    def work_flow(self):
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
