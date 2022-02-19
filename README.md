# neo4chain

neo4j etl for blockchains

Current chains: BTC, ETH

## Usage

Install latest python (>=3.9)

```bash
# edit config and get ready
cp config.example.json config.json
nano config.json

# if run eth
python eth

# if run btc
python btc 

```

## Daemon favor

The favored daemons will get long term support

| Chain | Daemon |
|---|---| 
| BTC | bitcoin-core |
| ETH | openethereum |
