# %%
from hexbytes.main import HexBytes
from neo4j import GraphDatabase
from neo4j.work import transaction
from neo4j.io import ClientError
from web3 import Web3
from ethereumetl.service.eth_contract_service import EthContractService
from token_transfer_extractor import EthTokenTransferExtractor

contract_service = EthContractService()
token_transfer_service = EthTokenTransferExtractor()

# w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8546'))
w3 = Web3(Web3.WebsocketProvider('wss://mainnet.infura.io/ws/v3/dc6980e1063b421bbcfef8d7f58ccd43'))
driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", "icaneatglass"))

print('using web3@'+w3.api)

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
    
    #Unique Constraint is already indexed
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
        CREATE (b)-[:BlockReward]->(addr)
        """, number=block.number, miner_addr=block.miner)

        # https://www.investopedia.com/terms/u/uncle-block-cryptocurrency.asp
        # Only one can enter the ledger as a block, and the other does not
        for uncle_idx in range(0, len(block.uncles)):
            uncle_block = w3.eth.get_uncle_by_block(block.number, uncle_idx)
            tx.run("""
            MATCH 
                (b:Block {number: $number}),
                (addr:Address:EOA {address: $miner_addr})
            CREATE (b)-[:UncleReward]->(addr)
            """, number=block.number, miner_addr=uncle_block.miner)

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
            CREATE (b)-[:Contains]->(tx)
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
        CREATE (from)-[:Send]->(tx)-[:To]->(to)
        """, {'hash':transaction.hash.hex(), 'from': transaction['from'], 'to':transaction['to']})
    else:
        print('tx {} has no to_addr'.format(transaction.hash.hex()))
        insert_Addr(tx, transaction['from'])
        tx.run("""
        MATCH (tx:Transaction {hash: $hash}),
            (from:Address {address: $from})
        CREATE (from)-[:Send]->(tx)
        """, {'hash':transaction.hash.hex(), 'from': transaction['from']})

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
    tx.run(query, address=addr, is_erc20=is_ERC20(bytecode), is_erc721=is_ERC721(bytecode))

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
        type: $type,
        gas: $gas,
        gasPrice: $gasPrice
    }) 
    """, {
        'hash':transaction.hash.hex(),
        'from': transaction['from'],
        'to': transaction['to'],
        'value': str(transaction['value']),
        'input': transaction['input'],
        'nonce': transaction['nonce'],
        'r': transaction['r'].hex(),
        's': transaction['s'].hex(),
        'v': transaction['v'],
        'type': transaction['type'],
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
    token_addr=transfer.token_address, 
    value=str(transfer.value))

    for addr in (transfer.from_address, transfer.to_address):
        insert_Addr(tx, addr)

    # add from replationships & add to replationships
    tx.run("""
        MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
            (from:Address {address: $from}),
            (to:Address {address: $to})
        CREATE (from)-[:SendToken]->(tf)-[:TokenTo]->(to)
        """, {'hash_idx': hash_idx, 'from': transfer.from_address, 'to': transfer.to_address})
    # add tx_hash replationships
    tx.run("""
        MATCH (tf:TokenTransfer {hash_idx: $hash_idx}),
            (tx:Transaction {hash: $hash})
        CREATE (tx)-[:CallTokenTransfer]->(tf)
        """, hash_idx= hash_idx, hash=transfer.transaction_hash)

def parse_TokenTransfer(tx, transaction_hash):
    # load token transfer from receipt logs
    logs = w3.eth.getTransactionReceipt(transaction_hash).logs
    for log in logs:
        transfer = token_transfer_service.extract_transfer_from_log(log)
        if transfer is not None:
            print('found token transfer')
            insert_TokenTransfer(tx, transfer)


# %% 
def get_local_height():
    results = db.run("MATCH (b:Block) RETURN max(b.number);").value()
    if results is None:
        return -1
    else:
        return results[0]

def work_flow():
    latest = w3.eth.getBlock('latest')
    while get_local_height()<latest:
        height = get_local_height() + 1
        block = w3.eth.getBlock(height)
        parse_Block(block)

    while get_local_height()<w3.eth.getBlock('latest'):
        height = get_local_height() + 1
        block = w3.eth.getBlock(height)
        parse_Block(block)

# %%

for block in range(2000100, 2000200):
    block = w3.eth.getBlock(block)
    parse_Block(block)
    print(block.number)

# %%
