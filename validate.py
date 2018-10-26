#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import inspect
import eospy.cleos
import pandas as pd
import numpy as np
import pprint
from multiprocessing import Pool

SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
pd.set_option('display.expand_frame_repr', False)
pd.options.display.max_rows = 999

parser = argparse.ArgumentParser()
parser.add_argument("-v", '--verbose', action="store_true",
                    dest="verbose", help='Print logged info to screen')
parser.add_argument("-d", '--debug', action="store_true",
                    dest="debug", help='Print debug info')
parser.add_argument('-l', '--log_file', default='{}.log'.format(
    os.path.basename(__file__).split('.')[0]), help='Log file')
parser.add_argument('-s', '--snapshot_file',
                    default='{}/eosmetal_telos_snapshot.csv'.format(SCRIPT_PATH), help='Snapshot file')
parser.add_argument('-u', '--api_endpoint',
                    default='http://127.0.0.1:8888', help='EOSIO API endpoint URI')
parser.add_argument('-t', '--num_threads', type=int,
                    default=4, help='Number of threads to get account balances')
args = parser.parse_args()

VERBOSE = args.verbose
DEBUG = args.debug
SNAPSHOT_FILE = args.snapshot_file
LOG_FILE = args.log_file
API_ENDPOINT = args.api_endpoint
NUM_THREADS = int(args.num_threads)

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
fh.setFormatter(formatter)
logger.addHandler(fh)

pp = pprint.PrettyPrinter(indent=2)
SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
cleos = eospy.cleos.Cleos(url=API_ENDPOINT)

def asset2float(asset):
    return float(asset.split(' ')[0])

def get_account_info(account):
    try:
        result = cleos.get_account(account)
        key = result['permissions'][0]['required_auth']['keys'][0]['key']
        balance = round(asset2float(result['core_liquid_balance']) + asset2float(result['total_resources']['cpu_weight']) + asset2float(result['total_resources']['net_weight']), 4)
        
        if len(result['permissions']) > 2:
            logger.critical('Account {} has more than 2 permissions'.format(account))
            return '', 0
        if result['permissions'][0]['required_auth']['keys'][0]['key'] != result['permissions'][0]['required_auth']['keys'][0]['key']:
            logger.critical('Owner and Active keys for account {} are different'.format(account))
            return '', 0
        if len(result['permissions'][0]['required_auth']['keys']) > 1 or len(result['permissions'][1]['required_auth']['keys']) > 1 or len(result['permissions'][0]['required_auth']['accounts']) > 0 or len(result['permissions'][1]['required_auth']['accounts']) > 0:
            logger.critical('Account {} has weird accounts or keys'.format(account))
            return '', 0

    except Exception as e:
        if 'unknown key' in str(e):
            logger.critical('Account {} is not on chain'.format(account))
        else:
            logger.critical(e)
        return '', 0

    #logger.debug('{} {} {}'.format(account, key, balance))
    return key, balance

def get_accounts(accounts):
    i= 0
    keys = []
    balances = []    
    
    with Pool(NUM_THREADS) as p:
        results = p.map(get_account_info, accounts)

    return pd.DataFrame(
    {'eos_account': accounts,
     'eos_key': [x[0] for x in results],
     'balance': ['{:.4f}'.format(x[1]) for x in results]
    }, dtype=str)

def main():
    try:
        telos_genesis = pd.read_csv(SNAPSHOT_FILE, dtype=str, names=['eth_address',
                                                                         'eos_account', 'eos_key', 'balance']).drop(columns=['eth_address']).sort_values(by=['eos_account'])
        telos_genesis = telos_genesis.reset_index(drop=True)                                                                        
    except Exception as e:
        logger.critical(
            'Error loading snapshot at {}: {}'.format(SNAPSHOT_FILE, e))
        exit(1)

    logger.info('Getting accounts from chain...')
    try:
        chain_accounts = get_accounts(telos_genesis['eos_account'].tolist()).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting acounts from chain: {}'.format(e))
        quit()

    if DEBUG:
        telos_genesis.to_csv('debug-genesis.csv', header=False)
        chain_accounts.to_csv('debug-chain.csv', header=False)

    logger.info('Checking accounts...')
    if telos_genesis.equals(chain_accounts):
        logger.info('All accounts in snapshot are present on chain with the right key and balance')
    else:
        ne_stacked = (telos_genesis != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(telos_genesis != chain_accounts)
        changed_from = telos_genesis.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('Accounts in genesis and chain don`t match')
        print(changes)
        quit()

    logger.info('Validation finished')
if __name__ == "__main__":
    main()
