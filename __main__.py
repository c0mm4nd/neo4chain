# %%
from hexbytes.main import HexBytes
from neo4j import GraphDatabase
from neo4j.io import ClientError
from web3 import Web3
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor
import time
import logging

logger = logging.getLogger(__name__)

contract_service = EthContractService()
token_transfer_service = EthTokenTransferExtractor()

# w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8546'))
w3 = Web3(Web3.WebsocketProvider(
    'wss://mainnet.infura.io/ws/v3/dc6980e1063b421bbcfef8d7f58ccd43'))
driver = GraphDatabase.driver(
    "bolt://127.0.0.1:7687", auth=("neo4j", "123456"))

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


def parse_Block(block):
    with db.begin_transaction() as tx:
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
               hash=block.hash.hex(),
               timestamp=block.timestamp,
               size=block.size,
               nonce=block.nonce.hex(),
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
            (addr:Address:EOA {address: $miner_addr})
        CREATE (b)-[:BLOCK_REWARD]->(addr)
        """, number=block.number, miner_addr=block.miner)  # TODO: BlockReward value

        # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
        # Only one can enter the ledger as a block, and the other does not
        for uncle_idx in range(0, len(block.uncles)):
            uncle_block = w3.eth.get_uncle_by_block(block.number, uncle_idx)
            tx.run("""
            MATCH 
                (b:Block {number: $number}),
                (addr:Address:EOA {address: $miner_addr})
            CREATE (b)-[:UNCLE_REWARD]->(addr)
            """, number=block.number, miner_addr=uncle_block.miner)  # TODO: UncleReward value

        for transaction_hash in block.transactions:
            if type(transaction_hash) is HexBytes:
                transaction_hash = transaction_hash.hex()
            transaction = w3.eth.getTransaction(transaction_hash)
            parse_Transaction(tx, transaction)

        for transaction_hash in block.transactions:
            if type(transaction_hash) is HexBytes:
                transaction_hash = transaction_hash.hex()
            parse_TokenTransfer(tx, transaction_hash)

        for transaction_hash in block.transactions:
            if type(transaction_hash) is HexBytes:
                transaction_hash = transaction_hash.hex()
            tx.run("""
            MATCH 
                (b:Block {number: $number}),
                (tx:Transaction {hash: $hash})
            CREATE (b)-[:CONTAINS]->(tx)
            """, number=block.number, hash=transaction_hash)


def parse_Transaction(tx, transaction):
    insert_Transaction(tx, transaction)

    if transaction.to != None:
        for addr in (transaction['from'], transaction['to']):
            insert_Addr(tx, addr)
        # insert relationships
        tx.run("""
        MATCH (tx:Transaction {hash: $hash}),
            (from:Address {address: $from}),
            (to:Address {address: $to})
        CREATE (from)-[:SEND]->(tx)-[:TO]->(to)
        """, {'hash': transaction.hash.hex(), 'from': transaction['from'], 'to': transaction['to']})
    else:
        insert_Addr(tx, transaction['from'])
        new_contract_address = get_new_contract_address(transaction.hash.hex())
        assert type(new_contract_address) == str and len(
            new_contract_address) > 0
        insert_Addr(tx, new_contract_address)
        logger.info('tx {} created a new contract {}'.format(
            transaction.hash.hex(), new_contract_address))

        tx.run("""
        MATCH (tx:Transaction {hash: $hash}),
            (from:Address {address: $from})
        CREATE (from)-[:SEND]->(tx)-[:CALL_CONTRACT_CREATION]->(new_contract)
        """, {'hash': transaction.hash.hex(), 'from': transaction['from'], 'new_contract_address': new_contract_address})


def get_new_contract_address(transaction_hash):
    receipt = w3.eth.getTransactionReceipt(transaction_hash)
    return receipt.contractAddress  # 0xabcd in str


def is_EOA(addr):
    code = w3.eth.getCode(Web3.toChecksumAddress(addr))
    return code == HexBytes('0x')


def insert_EOA(tx, addr):
    # todo: in tx
    query = "MERGE (a:Address:EOA{address: $address})"
    tx.run(query, address=addr)


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
    # todo: in tx
    query = """
    MERGE (a:Address:Contract{
        address: $address, 
        is_erc20:$is_erc20,
        is_erc721:$is_erc721
    })
    """
    bytecode = w3.eth.getCode(Web3.toChecksumAddress(addr)).hex()
    tx.run(query, address=addr, is_erc20=is_ERC20(
        bytecode), is_erc721=is_ERC721(bytecode))


def insert_Addr(tx, addr):
    if addr is None:
        raise ValueError("Address is None")
    if type(addr) is HexBytes:
        addr = addr.hex()
    if is_EOA(addr):
        insert_EOA(tx, addr)
    else:
        insert_Contract(tx, addr)


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
        'hash': transaction.hash.hex(),
        'from': transaction['from'],
        'to': transaction['to'],
        'value': str(transaction['value']),
        'input': transaction['input'],
        'nonce': transaction['nonce'],
        'r': transaction['r'].hex(),
        's': transaction['s'].hex(),
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
        insert_Addr(tx, addr)

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
def get_local_block_height():
    results = db.run("MATCH (b:Block) RETURN max(b.number);").value()
    if results[0] is None:
        return -1
    else:
        return results[0]


def get_local_block_timestamp():
    results = db.run("MATCH (b:Block) RETURN max(b.timestamp);").value()
    if results[0] is None:
        return -1
    else:
        return results[0]


def work_flow():
    latest = w3.eth.getBlock('latest').number
    local_height = get_local_block_height()
    while local_height < latest-1000:
        local_height += 1
        block = w3.eth.getBlock(local_height)
        parse_Block(block)
        logger.warning(f'{local_height}/{latest}')

    while True:
        latest = w3.eth.getBlock('latest').number
        local_timestamp = get_local_block_timestamp()
        while True:
            local_height += 1
            block = w3.eth.getBlock(local_height)
            if block.timestamp - local_timestamp < 60*60*24:
                break
            parse_Block(block)
            logger.warning(f'{local_height}/{latest}')
        time.sleep(60*60*24)  # every day


# %%


if __name__ == '__main__':
    work_flow()
