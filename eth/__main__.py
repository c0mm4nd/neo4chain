# %%
from hexbytes.main import HexBytes
from neo4j import GraphDatabase
from neo4j.io import ClientError
from web3 import Web3
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
from threading import Thread
import time
import logging
import os
import argparse

logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

contract_service = EthContractService()
token_transfer_service = EthTokenTransferExtractor()

# Websocket is not supported under multi thread https://github.com/ethereum/web3.py/issues/2090
# w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8546'))
# w3 = Web3(Web3.WebsocketProvider(
#     'wss://mainnet.infura.io/ws/v3/dc6980e1063b421bbcfef8d7f58ccd43'))
import requests
adapter = requests.adapters.HTTPAdapter(pool_connections=2**16, pool_maxsize=2**16)
session = requests.Session()
session.mount('http://', adapter)
session.mount('https://', adapter)
# w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545', session=session, request_kwargs={'timeout': 5}))
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/dc6980e1063b421bbcfef8d7f58ccd43', session=session, request_kwargs={'timeout': 5}))

driver = GraphDatabase.driver(
    "bolt://127.0.0.1:7687", auth=("neo4j", "icaneatglass"))

logger.warning('using web3@'+w3.api)


def drop_db():
    system = driver.session()
    system.run("DROP DATABASE ethereum")


def create_db():
    system = driver.session()
    system.run("CREATE DATABASE ethereum")
    system.close()

    db = driver.session(database="ethereum")
    db.run("CREATE CONSTRAINT block_hash_uq ON (block:Block) ASSERT block.hash IS UNIQUE")
    db.run("CREATE CONSTRAINT block_number_uq ON (block:Block) ASSERT block.number IS UNIQUE")
    db.run("CREATE CONSTRAINT addr_uq ON (addr:Address) ASSERT addr.address IS UNIQUE")
    db.run("CREATE CONSTRAINT tx_hash_uq ON (tx:Transaction) ASSERT tx.hash IS UNIQUE")
    db.run("CREATE CONSTRAINT tf_hash_idx_uq ON (tx:TokenTransfer) ASSERT tx.hash_idx IS UNIQUE")
    db.close()

    # Unique Constraint is already indexed
    # system.run("CREATE index block_hash_idx FOR (block:Block) ON (block.hash)")
    # system.run("CREATE index block_number_idx FOR (block:Block) ON (block.number)")
    # system.run("CREATE index addr_idx FOR (addr:Address) ON (addr.address)")
    # system.run("CREATE index tx_hash_idx FOR (tx:Transaction) ON (tx.hash)")


db = driver.session(database="ethereum")
try:
    db.run("create (placeholder:Block {height: -1})")
    db.run("MATCH  (placeholder:Block {height: -1}) delete placeholder")
except ClientError as e:
    if e.code == 'Neo.ClientError.Database.DatabaseNotFound':
        create_db()
    else:
        raise e

# %%


def parse_Block(tx, block):
    tx.run("""
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
            hash=block.hash if type(block.hash) is not HexBytes else block.hash.hex(),
            timestamp=block.timestamp,
            size=block.size,
            nonce= block.nonce if type(block.nonce) is not HexBytes else block.nonce.hex(),
            difficulty=str(block.difficulty),
            totalDifficulty=str(block.totalDifficulty),
            gasLimit=str(block.gasLimit),
            gasUsed=str(block.gasUsed))

    # miner must be an EOA
    insert_EOA(tx, block.miner)

    # todo: add reward amount into the reward relationships
    tx.run("""
        MATCH 
            (b:Block {number: $number}),
            (addr:Address {address: $miner_addr})
        CREATE (b)-[:BLOCK_REWARD]->(addr)
    """, number=block.number, miner_addr=block.miner)  # TODO: BlockReward value

    # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
    # Only one can enter the ledger as a block, and the other does not
    for uncle_idx in range(0, len(block.uncles)):
        uncle_block = w3.eth.get_uncle_by_block(block.number, uncle_idx)
        tx.run("""
            MATCH 
                (b:Block {number: $number}),
                (addr:Address {address: $miner_addr})
            CREATE (b)-[:UNCLE_REWARD]->(addr)
        """, number=block.number, miner_addr=uncle_block.miner)  # TODO: UncleReward value

    for transaction in block.transactions:
        if type(transaction) in (HexBytes, str):
            transaction = w3.eth.get_transaction(transaction)            
        parse_Transaction(tx, transaction)

    for transaction in block.transactions:
        if type(transaction) in (HexBytes, str):
            transaction_hash = transaction
        else:
            transaction_hash = transaction.hash
        parse_TokenTransfer(tx, transaction_hash)

    for transaction in block.transactions:
        if type(transaction) in (HexBytes, str):
            transaction_hash = transaction
        else:
            transaction_hash = transaction.hash
        tx.run("""
            MATCH 
                (b:Block {number: $number}),
                (tx:Transaction {hash: $hash})
            CREATE (b)-[:CONTAINS]->(tx)
        """, number=block.number, hash=transaction_hash)


def parse_Transaction(tx, transaction):
    insert_Transaction(tx, transaction)

    if transaction.to != None:
        insert_Address(tx, transaction['to'])  # to is unknown
        insert_EOA(tx, transaction['from'])  # from must be an EOA
        # insert relationships
        tx.run("""
        MATCH (tx:Transaction {hash: $hash}),
            (from:Address {address: $from}),
            (to:Address {address: $to})
        CREATE (from)-[:SEND]->(tx)-[:TO]->(to)
        """, {'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), 'from': transaction['from'], 'to': transaction['to']})
    else:
        insert_EOA(tx, transaction['from'])
        new_contract_address = get_new_contract_address(transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex())
        assert type(new_contract_address) == str and len(
            new_contract_address) > 0
        insert_Contract(tx, new_contract_address)
        logger.info('tx {} created a new contract {}'.format(
            transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), new_contract_address))

        tx.run("""
        MATCH (tx:Transaction {hash: $hash}),
            (from:Address {address: $from})
        CREATE (from)-[:SEND]->(tx)-[:CALL_CONTRACT_CREATION]->(new_contract)
        """, {'hash': transaction.hash if type(transaction.hash) is not HexBytes else transaction.hash.hex(), 'from': transaction['from'], 'new_contract_address': new_contract_address})


def get_new_contract_address(transaction_hash):
    receipt = w3.eth.getTransactionReceipt(transaction_hash)
    return receipt.contractAddress  # 0xabcd in str


def is_ERC20(bytecode):
    # contains bug here
    # https://github.com/blockchain-etl/ethereum-etl/issues/194
    # https://github.com/blockchain-etl/ethereum-etl/issues/195
    function_sighashes = contract_service.get_function_sighashes(bytecode)
    return contract_service.is_erc20_contract(function_sighashes)


def is_ERC721(bytecode):
    function_sighashes = contract_service.get_function_sighashes(bytecode)
    return contract_service.is_erc721_contract(function_sighashes)


def insert_Contract(tx, addr):
    if type(addr) is HexBytes:
        addr = addr.hex()
    query = """
    MERGE (a:Address {address: $address})
    set a.type = 2, a.is_erc20=$is_erc20, a.is_erc721=$is_erc721
    """
    bytecode = w3.eth.getCode(Web3.toChecksumAddress(addr))
    bytecode = bytecode if type(bytecode) is not HexBytes else bytecode.hex()

    tx.run(query, address=addr, is_erc20=is_ERC20(
        bytecode), is_erc721=is_ERC721(bytecode))


def insert_EOA(tx, addr):
    if type(addr) is HexBytes:
        addr = addr.hex()
    tx.run("""
    MERGE (a:Address {address: $address})
    set a.type = 1
    """, address=addr)


def insert_Address(tx, addr):
    if type(addr) is HexBytes:
        addr = addr.hex()
    query = "MERGE (a:Address {address: $address})"
    tx.run(query, address=addr)


def insert_Transaction(tx, transaction):
    # todo: in tx
    if type(transaction['transactionIndex']) is str and transaction['transactionIndex'].startswith('0x'):
        transaction['transactionIndex'] = int(
            transaction['transactionIndex'][2:], 16)

    tx.run("""
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


def insert_TokenTransfer(tx, transfer):
    # define hash_idx
    hash_idx = transfer.transaction_hash + '.' + str(transfer.log_index)

    # transfer struct
    # https://github.com/blockchain-etl/ethereum-etl/blob/develop/ethereumetl/domain/token_transfer.py#L24
    tx.run("""
    MERGE (a:TokenTransfer {
        hash_idx: $hash_idx,
        token_address: $token_addr,         
        value: $value
    })
    """, hash_idx=hash_idx,
           token_addr=transfer.token_address,  # do not add (Contract)-[handles]->[TokenTransfer] to avoid 1-INF too heavy relationship
           value=str(transfer.value))

    for addr in (transfer.from_address, transfer.to_address):
        insert_Address(tx, addr)

    # add from replationships & add to replationships
    tx.run("""
        MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
            (from:Address {address: $from}),
            (to:Address {address: $to})
        CREATE (from)-[:SEND_TOKEN]->(tf)-[:TOKEN_TO]->(to)
        """, {'hash_idx': hash_idx, 'from': transfer.from_address, 'to': transfer.to_address})
    # add tx_hash replationships
    tx.run("""
        MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
            (tx:Transaction {hash: $hash})
        CREATE (tx)-[:CALL_TOKEN_TRANSFER]->(tf)
        """, hash_idx=hash_idx, hash=transfer.transaction_hash)


def parse_TokenTransfer(tx, transaction_hash):
    # load token transfer from receipt logs
    logs = w3.eth.getTransactionReceipt(transaction_hash).logs
    for log in logs:
        transfer = token_transfer_service.extract_transfer_from_log(log)
        if transfer is not None:
            insert_TokenTransfer(tx, transfer)


# %%
def get_local_block_height(tx):
    results = tx.run("MATCH (b:Block) RETURN max(b.number);").value()
    if results[0] is None:
        return -1
    else:
        return results[0]


def get_local_block_timestamp(tx):
    results = tx.run(
        "MATCH (b:Block) with max(b.number) as top match (b:Block) where b.number = top return b.timestamp;").value()
    if results[0] is None:
        return -1
    else:
        return results[0]


def task_with_log(height, latest):
    with driver.session(database="ethereum") as session:
        try:
            save_requests = True
            session.write_transaction(parse_Block, w3.eth.get_block(height, full_transactions=save_requests))
            logger.warning(f'{height}/{latest}')
        except Exception as e:
            logger.error("failed to parse block on syncing")
            logger.error(e)
            os._exit(0)


def check_missing(local_height, safe_height=0):
    co = 100
    logger.warning(f'check missing blocks from {safe_height} to {local_height}')
    def task(height):
        with driver.session(database="ethereum") as session:
            results = session.run("MATCH (b:Block {number: $height}) RETURN b.number;", height=height).value()
            if type(results) is not list:
                logger.error(f"failed to inspect Block on {height}: results are {results}")
                os._exit(0)
            if len(results) != 1 or results[0] is None:
                logger.warning(f'Missing block {height}')
                while True:
                    retry = 0
                    try:
                        block = w3.eth.get_block(height, full_transactions=True)
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
                        session.write_transaction(parse_Block, block)
                        logger.warning(f"supplemented block {height}")
                        return
                    except Exception as e:
                        logger.error(f'parsing {height} failed')
                        logger.error(block)
                        logger.error(e)
                        if retry == 3:
                            os._exit(0)
                    time.sleep(2)
                    retry += 1
            else:
                if height % co != 0:
                    return
                logger.warning(f'Block {height} exists')
    height = safe_height
    while height < local_height:
        next_height = height + co
        if next_height > local_height:
            next_height = local_height
        tasks = [Thread(target=task, args=(i,)) for i in range(height, next_height)]
        for t in tasks:
            t.start()
        for t in tasks:
            t.join()
        height = next_height


def work_flow(safe_height=-1):
    logger.warning('Start workflow')
    latest = w3.eth.get_block('latest', full_transactions=False).number
    db = driver.session(database="ethereum")
    local_height = get_local_block_height(db)
    if local_height > 0:
        if safe_height < 0: 
            safe_height =  local_height - 1000 if local_height > 1000 else 0
        check_missing(local_height, safe_height)

    co = 100
    if local_height < latest -1000:
        logger.warning(f'running on loading mode')

    while local_height < latest-1000:
        tasks = [Thread(target=task_with_log, args=(local_height+i+1, latest)) for i in range(co)]
        
        # start all
        for t in tasks:
            t.start()
        for t in tasks:
            t.join()
        local_height += co

    while True:
        logger.warning(f'running on sync mode')
        latest = w3.eth.get_block('latest', full_transactions=False).number
        local_timestamp = get_local_block_timestamp(db)
        while True:
            local_height += 1
            block = w3.eth.getBlock(local_height, full_transactions=True)
            if block.timestamp - local_timestamp < 60*60*24:
                break
            with driver.session(database="ethereum") as db:
                parse_Block(db, block)
            logger.warning(f'{local_height}/{latest}')
        time.sleep(60*60*24)  # every day


# %%


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
 
    # Adding optional argument
    parser.add_argument("-s", "--safe-height", type=int, default=-1, help = "Check missing block from this height")
    args = parser.parse_args()
    work_flow(safe_height=args.safe_height)
