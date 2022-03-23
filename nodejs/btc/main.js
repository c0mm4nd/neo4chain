import { driver, session } from 'neo4j-driver'
import { Neo4jError } from 'neo4j-driver-core/lib/error.js'
import { Client } from 'bitcoin-simple-rpc'
import { createLogger } from 'winston'
import { isMainThread } from 'worker_thread'

const logger = createLogger({
    level: 'info',
    defaultMeta: { service: 'btc' },
    transports: [
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' }),
    ],
});

class BitcoinETL {
    constructor(config) {
        this.config = config
        rpc_config = config["daemon"]  // required
        neo4j_config = config["neo4j"]  // required

        this.rpc = new Client(
            rpc_config["address"], rpc_config["username"], rpc_config["password"], parse_float = float)
        this.driver = driver(
            neo4j_config["address"], auth = (neo4j_config["username"], neo4j_config["password"]))
        this.dbname = neo4j_config["database"] || "btc"

        this.ensure_db_exists()
    }

    async create_db() {
        system_session = driver.session()
        await system_session.run('create database ')
        await system_session.close()

        session = driver.session(database = this.dbname)
        await session.run(
            "CREATE CONSTRAINT ON (c:Coinbase) ASSERT c.id IS UNIQUE;")
        await session.run(
            "CREATE CONSTRAINT ON (b:Block) ASSERT b.hash IS UNIQUE;")
        await session.run(
            "CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;")
        await session.run(
            "CREATE CONSTRAINT ON (o:Output) ASSERT (o.height, o.txid, o.n) IS NODE KEY;")
        await session.run(
            "CREATE CONSTRAINT FOR (tx:Transaction) REQUIRE (tx.height, tx.txid) IS UNIQUE;")
        // txid is not unique
        await session.run("CREATE INDEX for (tx:Transaction) on (tx.txid);")
        await session.run("CREATE INDEX for (o:Output) on (o.txid, o.n);")
        await session.run("CREATE (:Coinbase) ")  // coinbase node
        await session.close()
    }

    async ensure_db_exists() {
        try {
            session = this.driver.session(database = this.dbname)
            await session.run("create (placeholder:Block {height: -1})")
            await session.run(
                "MATCH  (placeholder:Block {height: -1}) delete placeholder")
            // ensure_coinbase_addr_exists
            await session.run("MERGE (:Coinbase) ")
        } catch (err) {
            if (err instanceof Neo4jError) {
                this.create_db()
            } else {
                throw err
            }
        }
    }

    async parse_block(block, is_genesis = False) {
        if (is_genesis) {
            block["previousblockhash"] = null
        }
        session = this.driver.session({ database: this.dbname })
        await session.run(`
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
        })`, { ...block })

        logger.warning(`concurrently processing ${len(block["tx"])} txs in block ${block["height"]}`)
        block["tx"].forEach((tx, index) => {
            this.parse_tx(tx)
        })
        await session.close()
    }

    async parse_tx(session, block, tx) {
        const query = `
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
        })`
        if (tx.fee == null) {
            await session.run(query, { ...tx, height: block['height'], fee: null })
        } else {
            await session.run(query, { ...tx, height: block['height'] })
        }

        tx['vin'].forEach((vin, index) => {
            if (vin['coinbase'] == null && len(vin['coinbase']) > 0) {
                await session.run(`
                MATCH (o:Coinbase) 
                WITH o
                CREATE (vin:Input {
                    coinbase: $coinbase,
                    txinwitness: $txinwitness,
                    sequence: $sequence
                })
                CREATE (o)-[:SPENT_BY]->(vin)
                CREATE (vin)-[:VIN]->(:Transaction {txid: $txid})`,
                    {
                        coinbase: vin['coinbase'],
                        txinwitness: vin["txinwitness"] == null ? null : vin["txinwitness"].join(','),
                        sequence: vin["sequence"],
                        txid: tx["txid"],
                    })
            } else {
                await session.run(`
                MATCH (o:Output {txid: $out_txid, n: $out_n}) 
                WITH o
                CREATE (vin:Input {
                    // txid: txid,
                    // vout: vout,
                    scriptSig: $scriptSig,
                    txinwitness: $txinwitness,
                    sequence: $sequence
                })
                CREATE (o)-[:SPENT_BY]->(vin)
                CREATE (vin)-[:VIN]->(:Transaction {txid: $txid})
                `, {
                    out_txid: vin['txid'],
                    out_n: vin['vout'],
                    // txid=vin["txid"],
                    // vout=vin["out"],
                    scriptSig: vin["scriptSig"] == null ? null : vin["scriptSig"]["hex"],
                    txinwitness: vin["txinwitness"] == null ? null : vin["txinwitness"].join(','),
                    sequence: vin["sequence"], txid: tx["txid"]
                })
            }
        })

        tx['vout'].forEach((vout, index) => {
            switch (vout['scriptPubKey']['type']) {
                case 'nulldata':
                case 'nonstandard':
                    await session.run(`
                    CREATE (o:Output {
                        type: $type,
                        height: $height,
                        txid: $txid,
                        n: $n,
                        value: $value,
                        hex: $hex
                    })
                    CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
                    `, {
                        type: vout['scriptPubKey']['type'],
                        height: block["height"],
                        txid: tx["txid"], n: vout["n"],
                        value: vout["value"],
                        hex: vout['scriptPubKey']['hex'],
                    })
                    break
                case 'pubkeyhash':
                case 'scripthash':
                    this.save_address(session, vout['scriptPubKey']['address'])
                    await session.run(`
                    MATCH (a:Address {address: $address})
                    WITH a
                    CREATE (o:Output {
                        type: "address",
                        height: $height,
                        txid: $txid,
                        n: $n,
                        value: $value
                    })
                    CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
                    CREATE (o)-[:TO]->(a)
                    `, {
                        height: block["height"],
                        txid: tx["txid"], n: vout["n"], value: vout["value"],
                        address: vout['scriptPubKey']['address'],
                    })
                    break
                case 'pubkey':
                    public_key = vout['scriptPubKey']['asm'].split(" ")[0]
                    descriptor = await this.rpc.getdescriptorinfo(`pkh(${public_key})`)['descriptor']
                    addr = await this.rpc.deriveaddresses(descriptor)[0]
                    await this.save_address(t, addr, public_key = vout['scriptPubKey']['hex'])
                    await session.run(`
                    MATCH (a:Address {address: $address})
                    WITH a
                    CREATE (o:Output {
                        type: "address",
                        height: $height,
                        txid: $txid,
                        n: $n,
                        value: $value
                    })
                    CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
                    CREATE (o)-[:TO]->(a)
                    `, {
                        height: block["height"],
                        txid: tx["txid"],
                        n: vout["n"], value: vout["value"],
                        address: addr
                    })
                    break
                case 'multisig':
                    asms = vout['scriptPubKey']['asm'].split(" ")
                    public_keys = asms.filter((publicKey) => { publicKey.startsWith("04") || publicKey.startsWith("03") || publicKey.startsWith("02") })
                    public_keys_required = int(asms[0])
                    // public_keys_required <= len(public_keys)
                    multisig_address = await this.rpc.createMultiSig(public_keys_required, public_keys)["address"] // legacy address (p2sh)
                    await this.save_address(t, multisig_address, multisig = vout['scriptPubKey']['asm'])

                    public_keys.forEach((public_key) => {
                        descriptor = await this.rpc.getdescriptorinfo(
                            `pkh(${public_key})`)['descriptor']
                        addr = await this.rpc.deriveaddresses(descriptor)[0]
                        await this.save_address(t, addr, public_key = vout['scriptPubKey']['hex'])
                        await session.run(`
                        MATCH (multisig:Address {address: $multisig_address}), (sub:Address {address: $sub})
                        WITH multisig, sub
                        CREATE (multisig)-[:CONTROLLED_BY]->(sub)
                        `, {
                            multisig_address: multisig_address,
                            sub: addr,
                        })
                    })
                    await session.run(`
                    MATCH (a:Address {address: $multisig_address})
                    WITH a
                    CREATE (o:Output {
                        type: "address",
                        height: $height,
                        txid: $txid,
                        n: $n,
                        value: $value
                    })
                    CREATE (:Transaction {txid: $txid})-[:VOUT]->(o)
                    CREATE (o)-[:TO]->(a)
                    `, {
                        height: block["height"],
                        txid: tx["txid"], n: vout["n"],
                        value: vout["value"],
                        multisig_address: multisig_address
                    })
                    break
                default:
                    logger.error('unknown type: "{}" on tx {}'.format(
                        vout["scriptPubKey"]["type"], tx["txid"]))
                    process.exit(1)
            }
        })
    }

    async save_address(session, addr, public_key, multisig) {
        result = await this.rpc.validateAddress(addr)
        if (!result['isvalid']) {
            logger.error("invalid address: " + addr)
            process.exit(1)
        } else {
            await session.run(`
            MERGE (a:Address {
                address: $address,
                scriptPubKey: $scriptPubKey,
                isscript: $isscript,
                iswitness: $iswitness
            })
            `, { ...result })
            if (public_key) {
                await session.run(`
                MATCH (a:Address {address: $address})
                WHERE a.pubkey IS NULL
                SET a.pubkey = $pubkey
                RETURN count(a)
                `, {
                    address: addr,
                    pubkey: public_key
                })
            }
            if (multisig) {
                await session.run(`
                MATCH (a:Address {address: $address})
                WHERE a.multisig IS NULL
                SET a.multisig = $multisig
                RETURN count(a)
                `, {
                    address: addr,
                    multisig: multisig
                }
                )
            }
        }
    }

    async get_remote_top_height() {
        info = await this.rpc.getBlockchainInfo()
        return info['blocks']
    }

    async get_block_by_height() {
        retry = 0
        while (true) {
            try {
                hash = await this.rpc.getblockhash(height)
                // verbosity 2 will carry the tx entities
                block = await this.rpc.getblock(hash, 2)
                return block
            } catch (e) {
                logger.warning(e)
                if (retry == 3) process.exit(0)
                retry += 1
            }
        }
    }

    async get_local_top_block(tx) {
        results = await tx.run(
            "MATCH (b:Block) RETURN b order by b.height desc limit 1;").records
        if (results == null || len(results) == 0) {
            return null
        }
        return results.get(0)
    }

    async supplement_missing(block) {
        height = block["height"]
        session = this.driver.session(database = "btc")
        if (!(await session.readTransaction(this.block_exists, height))) {
            logger.error(`cannot find block ${height}`)
            process.exit(0)
        }
        // checking txs
        tx_number = len(block["tx"])
        logger.warning(`checking missing tx(${tx_number} in total) for block ${height}`)
        block["tx"].forEach(tx => await this.supplement_missing_tx_task(block, tx))
        await session.close()
    }

    async block_exists(t, height) {
        results = await t.run(
            "MATCH (b:Block {height: $height}) RETURN b limit 1", { height: height }).records
        return (results && len(results) != 0 && results[0] == 1)
    }

    async block_tx_exists(t, height, txid) {
        results = t.run(
            "MATCH (tx:Transaction {height: $height, txid: $txid}) RETURN count(tx)", height = height, txid = txid).records
        return results && len(results) != 0 && results[0] != 0
    }

    async supplement_missing_tx_task(block, tx) {
        session = await this.driver.session(database = this.dbname)
        height = block["height"]
        txid = tx["txid"]
        if (!await session.readTransaction(this.block_tx_exists, height, txid)) {
            logger.warning(
                `txid ${txid} on ${height} is not found, supplement...`)
            await session.writeTransaction(this.parse_tx, block, tx)
            logger.warning(`txid ${txid} on ${height} is supplemented`)
        }
    }

    async work_flow() {
        remote_top = await this.get_remote_top_height()
        target = (remote_top - 1000) // 1000 * 1000

        var session = this.driver.session(database = this.dbname)
        top_block = await session.read_transaction(this.get_local_top_block)

        local_height = 0
        if (top_block) local_height = top_block['height']
        else {
            logger.warning("processing genesis block")
            this.parse_block(await this.get_block_by_height(0), is_genesis = True)
        }

        if (this.config["checker"] && local_height > 0) {
            co = this.config["checker"]["thread"] || 100
            logger.warning(`running on check missing mode, thread ${co}`)
            safe_height = this.config["checker"]["safe-height"]
            if (safe_height == null) await this.supplement_missing(
                await this.get_block_by_height(top_block["height"]))
            else {
                end = top_block["height"]
                logger.warning(`checking from ${safe_height} to ${end}`)
                for (height = safe_height + 1; height < end; height++) {
                    await this.supplement_missing(await this.get_block_by_height(height))
                }
            }
        }
        if (this.config["syncer"] && local_height < target) {
            co = this.config["syncer"]["thread"] || 100
            logger.warning(`running on sync mode, thread ${co}`)
            for (height = local_height + 1; height < target; height++) {
                block = await this.get_block_by_height(height)
                logger.warning(`processing block(with ${len(block["tx"])} txs): ${height} -> ${target}(remote ${remote_top})`)
                // session.write_transaction(parse_block, rpc, block)
                await this.parse_block(block)
                // if top_node is not None:
                //     t.create(Relationship(block_node, "PREV", top_node))
            }
        }
    }
}

async function main() {
    // const driver = driver('bolt://localhost:7687', neo4j.auth.basic('neo4j', 'icaneatglass'))

    // const session = driver.session()
    // await session.close()
    await driver.close()
}

if (isMainThread) {
    await main()
}
