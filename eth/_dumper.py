from hexbytes import HexBytes
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
from ethereumetl.service.eth_contract_service import EthContractService
from web3 import Web3
import json
import requests
import argparse

parser = argparse.ArgumentParser()

# Adding optional argument
parser.add_argument("-c", "--contract", type=str,
                    default="0x", help="The contract hash")
parser.add_argument("-s", "--start", type=int,
                    default=0, help="The start of scanner")
args = parser.parse_args()


target = args.contract
start = args.start

adapter = requests.adapters.HTTPAdapter(
    pool_connections=2**16, pool_maxsize=2**16)
session = requests.Session()
session.mount('http://', adapter)
session.mount('https://', adapter)
w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545',
          session=session, request_kwargs={'timeout': 20}))
block_txs = {}

token_transfer_service = EthTokenTransferExtractor()

latest = w3.eth.get_block(
    'latest', full_transactions=False).number

f = open(f'{target}_txs.json', 'w', encoding='utf-8')
for height in range(start, latest):
    print(f'start scanning block {height}')
    block = w3.eth.get_block(
        height, full_transactions=True)
    for transaction in block.transactions:
        if type(transaction) in (HexBytes, str):
            transaction_hash = transaction
        else:
            transaction_hash = transaction.hash
        logs = w3.eth.getTransactionReceipt(transaction_hash).logs
        for log in logs:
            transfer = token_transfer_service.extract_transfer_from_log(log)
            if transfer is not None and transfer.token_address == target:
                if block_txs.get(height) is None:
                    block_txs[height] = []
                block_txs[height].append(transfer.__dict__)
    json.dump(block_txs, f, ensure_ascii=False, indent=4)
