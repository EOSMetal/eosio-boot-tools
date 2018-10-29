#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import inspect
import hashlib
from eospy.cleos import Cleos

SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
    
parser = argparse.ArgumentParser()
parser.add_argument("-v", '--verbose', action="store_true",
                    dest="verbose", help='Print logged info to screen')
parser.add_argument("-d", '--debug', action="store_true",
                    dest="debug", help='Print debug info')
parser.add_argument('-l', '--log_file', default='{}.log'.format(
    os.path.basename(__file__).split('.')[0]), help='Log file')
parser.add_argument('-u', '--api_endpoint',
                    default='http://127.0.0.1:8888', help='EOSIO API endpoint URI')
parser.add_argument('-c', '--contracts_path',
                    default='/opt/telos-launch/source/telos/build/contracts/', help='Path of the compiled contracts')

args = parser.parse_args()
VERBOSE = args.verbose
DEBUG = args.debug
LOG_FILE = args.log_file
API_ENDPOINT = args.api_endpoint
CONTRACTS_PATH = args.contracts_path

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

cleos = Cleos(url=API_ENDPOINT)
def sha256sum(filename):
    h = hashlib.sha256()
    with open(filename, 'rb', buffering=0) as f:
        for b in iter(lambda: f.read(128*1024), b''):
            h.update(b)
    return h.hexdigest()

def main():
  for contract in CONTRACTS:
    try:
      chain_hash = cleos.get_code(contract['account'])['code_hash']
      contract_hash = sha256sum('{}{}/{}.wasm'.format(CONTRACTS_PATH, contract['contract'], contract['contract']))
    
      if chain_hash == contract_hash:
        logger.info('Contract {} for account {} matches'.format(contract['contract'], contract['account']))
      else:
        logger.critical('Contract {} for account {} doesn\'t match'.format(contract['contract'], contract['account']))
    except Exception as e:
      logger.critical('Error checking contract: {}'.format(e))
      quit()

if __name__ == "__main__":
    main()
