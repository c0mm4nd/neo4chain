# %%
import argparse
import json
import logging

from etl import BitcoinETL

logger = logging.getLogger(__name__)

if __name__ == "__main__":
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
        config = json.load(file)
    btc_config = config["btc"]

    etl = BitcoinETL(config=btc_config)
    etl.work_flow()
