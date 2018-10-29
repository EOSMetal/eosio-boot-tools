#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import inspect

SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
    
parser = argparse.ArgumentParser()
parser.add_argument("-v", '--verbose', action="store_true",
                    dest="verbose", help='Print logged info to screen')
parser.add_argument("-d", '--debug', action="store_true",
                    dest="debug", help='Print debug info')
parser.add_argument('-l', '--log_file', default='{}.log'.format(
    os.path.basename(__file__).split('.')[0]), help='Log file')
args = parser.parse_args()

VERBOSE = args.verbose
DEBUG = args.debug
LOG_FILE = args.log_file


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s')
if DEBUG:
    logger.setLevel(logging.DEBUG)
if VERBOSE:
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

fh = logging.FileHandler(LOG_FILE)
logger.addHandler(fh)
fh.setFormatter(formatter)

SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))

CONTRACTS = [
  {
    'account': 'eosio',
    'contract': 'eosio.system'
  },
  {
    'account': 'eosio.token',
    'contract': 'eosio.token'
  },
  {
    'account': 'eosio.msig',
    'contract': 'eosio.msig'
  }
]

cleos = Cleos(url='http://api.pennstation.eosnewyork.io:7001')

def main():
  for contract in CONTRACTS:
    logger.info('Verifying {} contract'.format(contract['contract']))


if __name__ == "__main__":
    main()
