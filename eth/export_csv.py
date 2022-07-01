# %% script for extracting data for neo4j admin import (initial)
import json
from pathlib import Path
import threading
import atexit
from hexbytes.main import HexBytes
from web3 import Web3
from helpers import hex_to_int
from reward_calculator import get_const_reward, get_uncle_reward
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
from concurrent.futures import ThreadPoolExecutor, wait
from dask import dataframe as dd
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

class EthereumCSVETL:
    def __init__(self, config):
        self.config = config
        rpc_config = config["daemon"]

        self.addresses = {}
        self.csv = {}
        self.writter = {}

        self.contract_service = EthContractService()
        self.token_transfer_service = EthTokenTransferExtractor()

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=2**16, pool_maxsize=2**16)
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.w3 = Web3(Web3.HTTPProvider(rpc_config["address"],
                                         session=session, request_kwargs={'timeout': 20}))
        logger.warning('using web3@'+self.w3.api)
    
    def exit_handler(self):
        address_csv = open(os.path.join("import", 'address.csv'), 'w')
        address_writter = TSDictWriter(address_csv, fieldnames=[
            'address', 'type', 'is_erc20', 'is_erc721'])
        address_writter.writeheader()
        
        for address in self.addresses:
            address_writter.writerow(self.addresses[address])
        
        address_csv.close()
            

    def get_hash(self, block_or_tx):
        if type(block_or_tx) is str:
            return block_or_tx
        elif type(block_or_tx) is HexBytes:
            return block_or_tx.hex()
        else:
            return self.get_hash(block_or_tx.hash)

    def enhance_block(self, block):
        block.__dict__["uncle_blocks"] = []
        for uncle_idx in range(0, len(block.uncles)):
            uncle_block = self.w3.eth.get_uncle_by_block(
                block.number, uncle_idx)
            block["uncle_blocks"].append(uncle_block)

        reward = get_const_reward(block["number"])

        block.__dict__["created_contracts"] = {}
        block.__dict__["transfers"] = {}
        block.__dict__["transaction_receipt"] = {}
        for transaction in block.transactions:
            transaction_hash = self.get_hash(transaction)
            if not transaction.to:
                new_contract_address = self.get_new_contract_address(
                    transaction_hash)
                block["created_contracts"][transaction_hash] = new_contract_address
                logger.info('tx {} created a new contract {}'.format(
                            transaction_hash, new_contract_address))

            block["transfers"][transaction_hash] = []
            receipt = self.w3.eth.get_transaction_receipt(transaction_hash)
            block.__dict__["transaction_receipt"][transaction_hash] = receipt
            logs = receipt.logs
            for log in logs:
                transfer = self.token_transfer_service.extract_transfer_from_log(
                    log)
                if transfer:
                    block["transfers"][transaction_hash].append(transfer)
            fee = receipt.gasUsed * transaction.gasPrice
            reward += fee
        block.__dict__["reward"] = reward
        return block

    def ensure_block_Addresses(self, block):
            self.insert_Address_EOA(block.miner)
            for uncle_block in block["uncle_blocks"]:
                self.insert_Address_EOA(uncle_block['miner'])
            for transaction in block.transactions:
                transaction_hash = self.get_hash(transaction)
                if transaction.to:
                    # from must be an EOA
                    self.insert_Address_EOA(transaction['from'])
                    if len(block.__dict__["transaction_receipt"][transaction_hash].logs) > 0:
                        self.insert_Address_Contract(transaction['to'])
                    else:
                        self.insert_Address_Unknown(
                            transaction['to'])  # to is unknown
                else:
                    self.insert_Address_EOA(transaction['from'])
                    self.insert_Address_Contract(
                        block["created_contracts"][transaction_hash])

                for transfer in block["transfers"][transaction_hash]:
                    for addr in (transfer.from_address, transfer.to_address):
                        self.insert_Address_Unknown(addr)

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
        if self.addresses.get(addr) is None or self.addresses[addr]["type"] == "Unknown":
            bytecode = self.w3.eth.getCode(Web3.toChecksumAddress(addr))
            bytecode = bytecode if type(
            bytecode) is not HexBytes else bytecode.hex()
            self.addresses[addr] = {
                "type": "Contract",
                'address': addr, 
                'is_erc20': 1 if self.is_ERC20(bytecode) else 0, 
                'is_erc721': 1 if self.is_ERC721(bytecode) else 0,
                }

    def insert_Address_EOA(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        if self.addresses.get(addr) is None or self.addresses[addr]["type"] == "Unknown":
            self.addresses[addr] = {
                "address": addr,
                "type": "EOA"}

    def insert_Address_Unknown(self, addr):
        if type(addr) is HexBytes:
            addr = addr.hex()
        if self.addresses.get(addr) is None:
            self.addresses[addr] = {
                "address": addr,
                "type": "Unknown"}

    def insert_Transaction(self, block_hash, transaction):
        if type(transaction['transactionIndex']) is str and transaction['transactionIndex'].startswith('0x'):
            transaction['transactionIndex'] = int(
                transaction['transactionIndex'][2:], 16)

        self.writter['transaction'].writerow({
            'block_hash': block_hash,
            'hash':  transaction['hash'] if type(transaction['hash']) is not HexBytes else transaction['hash'].hex(),
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
        # transfer struct
        # https://github.com/blockchain-etl/ethereum-etl/blob/develop/ethereumetl/domain/token_transfer.py#L24
        self.writter['transfer'].writerow({'transaction_hash': transfer.transaction_hash,
                                           'log_index': transfer.log_index,
                                           # do not add (Contract)-[handles]->[TokenTransfer] to avoid 1-INF too heavy relationship
                                           'token_addr': transfer.token_address,
                                           'value': str(transfer.value),
                                           'value_raw': transfer.value_raw,
                                           'from_address': transfer.from_address,
                                           'to_address': transfer.to_address
                                           })
        
    def parse_block_header(self, block):
        block_hash = self.get_hash(block)
        self.writter['block'].writerow({
                        "number": block.number,
                        "miner": block.miner,
                        "reward": block["reward"],
                        "hash": block_hash,
                        "timestamp": block.timestamp,
                        "size": block.size,
                        "nonce": block.nonce if type(
                            block.nonce) is not HexBytes else block.nonce.hex(),
                        "difficulty": str(block.difficulty),
                        "totalDifficulty": str(block.totalDifficulty),
                        "gasLimit": str(block.gasLimit),
                        "gasUsed": str(block.gasUsed)
                        })
        
        # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
        # Only one can enter the ledger as a block, and the other does not
        for uncle_block in block["uncle_blocks"]:
            self.writter['block_uncle_rewards'].writerow({
                "block_hash": block_hash,
                "miner_addr": uncle_block.miner,
                "reward": str(get_uncle_reward(block['number'], hex_to_int(uncle_block['number'])))
            })
            
    def parse_block_tx(self, block, transaction):
        block_hash = self.get_hash(block)
        transaction_hash = self.get_hash(transaction)

        self.insert_Transaction(block_hash, transaction)

        for transfer in block["transfers"][transaction_hash]:
            self.insert_TokenTransfer(transfer)

    def thread_task(self, block_number, latest, tx_executor):
        block = self.w3.eth.get_block(
                block_number, full_transactions=True)
        block = self.enhance_block(block)
            
        logger.warning(
                'processing block(with {} txs) {} -> {}'.format(
                    len(block.transactions), block_number, latest
                ))

        self.ensure_block_Addresses(block)
        self.parse_block_header(block)
        wait([tx_executor.submit(self.parse_block_tx, block, transaction)
              for transaction in block.transactions])

    def init_csv(self):
        # self.csv['address'] = open(os.path.join("import", 'address.csv'), 'w')
        self.csv['block'] = open(os.path.join("import", 'block.csv'), 'w')    
        self.csv['block_uncle_rewards'] = open(os.path.join("import", 'block_uncle_rewards.csv'), 'w')   
        self.csv['transaction'] = open(os.path.join("import", 'transaction.csv'), 'w') 
        self.csv['transfer'] = open(os.path.join("import", 'transfer.csv'), 'w')        

        # self.writter['address'] = TSDictWriter(self.csv['address'], fieldnames=[
        #     'address', 'type', 'is_erc20', 'is_erc721'])
        self.writter['block'] = TSDictWriter(self.csv['block'], fieldnames=[
            'number', "miner", 'hash', "reward", 'timestamp', 'size', 'nonce', 'difficulty', 'totalDifficulty', 'gasLimit', 'gasUsed'
        ])
        self.writter['transaction'] = TSDictWriter(self.csv['transaction'], fieldnames=[
            'block_hash', 'hash', 'from', 'to', 'value', 'input', 'nonce', 'r', 's', 'v', 'transactionIndex', 'gas', 'gasPrice'])
        self.writter['transfer'] = TSDictWriter(self.csv['transfer'], fieldnames=[
            'transaction_hash', 'log_index', 'token_addr', 'value'])
        self.writter['block_uncle_rewards'] = TSDictWriter(self.csv['block_uncle_rewards'], fieldnames=[
            'block_hash', 'miner_addr', 'reward'])
        
        for key in self.writter:
            self.writter[key].writeheader()

    def close_csv(self):
        for key in self.csv:
            self.csv[key].close()

    # def save_csv(self):
    #     for key in self.csv:
    #         tmp_file = self.csv[key]
    #         filename = os.path.join(
    #             "import",
    #             Path(tmp_file.name).stem,
    #         )
    #         with open(tmp_file.name, 'r') as tmp, open(filename, 'a') as f:
    #             f.writelines(tmp.readlines())

    def save_height(self, height):
        filename = os.path.join("import", "height")
        with open(filename, "w") as f:
            f.write(str(height))

    def load_height(self):
        filename = os.path.join("import", "height")
        try:
            with open(filename) as f:
                content = f.readline()
                return int(content)
        except:
            return -1

    def work_flow(self):
        co = 256
        latest = self.w3.eth.get_block(
            'latest', full_transactions=False).number
        self.init_csv()
        with ThreadPoolExecutor(max_workers=co) as tx_executor:
            for number in range(0, latest-1000):
                self.thread_task(number, latest, tx_executor)
                # self.save_csv()
                
            self.close_csv()
        self.exit_handler()

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
    atexit.register(etl.exit_handler)
    etl.work_flow()
    
