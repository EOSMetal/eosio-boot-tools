#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import inspect
import eospy.cleos
import pandas


SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))

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
parser.add_argument('-b', '--batch_size', type=int,
                    default=200, help='Number of actions per transaction')
args = parser.parse_args()

VERBOSE = args.verbose
DEBUG = args.debug
SNAPSHOT_FILE = args.snapshot_file
LOG_FILE = args.log_file
API_ENDPOINT = args.api_endpoint
BATCH_SIZE = int(args.batch_size)

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

RAM_KB = 4 
SYMBOL = "TLOS"
KEY = "5JRiK3ctuSgPwEsFvY1FeCxW6VHYcGo3h28YkyJYBnEBvtgrhPd"
SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
cleos = eospy.cleos.Cleos(url=API_ENDPOINT)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def get_account_creation_actions(account, balance, key):
    # Create new account tx
    owner_active_auth = {
        "threshold": 1,
        "keys": [{
            "key": key,
            "weight": 1
        }],
        "accounts": [],
        "waits": []
    }
    newaccount_payload = {
        'creator' : 'eosio', 
        'name' : account, 
        'owner': owner_active_auth, 
        'active': owner_active_auth
    }
    newaccount_data = cleos.abi_json_to_bin('eosio', 'newaccount', newaccount_payload)
    newaccount_action = {
        'account' : 'eosio',
        'name' : 'newaccount',
        'authorization' : [
        {
            'actor' : 'eosio',
            'permission' : 'active'
        } ],
        'data' : newaccount_data['binargs']
    }

    # Create buy ram tx
    buyram_payload = {
        'payer':'eosio', 
        'receiver':account, 
        'bytes': RAM_KB*1024
    }
    buyram_data = cleos.abi_json_to_bin('eosio', 'buyrambytes', buyram_payload)
    buyram_action = {
        'account' : 'eosio',
        'name' : 'buyrambytes',
        'authorization' : [
            {
                'actor' : 'eosio',
                'permission' : 'active'
            } ],
        'data' : buyram_data['binargs']
    }

    # Create delegatebw tx
    f_balance = float(balance)
    f_balance = float(balance)
    if f_balance < 3:
        liquid = 0.1
    elif f_balance <= 11:
        liquid = 2.0
    else:
        liquid = 10.0
    
    remainder = f_balance - liquid
    delegate_cpu = round(remainder / 2, 4)
    delegate_net = remainder - delegate_cpu

    delegate_payload = {
        'from': 'eosio', 
        'receiver': account, 
        'stake_net_quantity': '{:.4f} {}'.format(delegate_net, SYMBOL), 
        'stake_cpu_quantity': '{:.4f} {}'.format(delegate_cpu, SYMBOL), 
        'transfer': True 
    }
    delegate_data = cleos.abi_json_to_bin('eosio', 'delegatebw', delegate_payload)
    delegate_action = {
        'account' : 'eosio',
        'name' : 'delegatebw',
        'authorization' : [
            {
                'actor' : 'eosio',
                'permission' : 'active'
            } ],
        'data' : delegate_data['binargs']
    }

    # Create transfer tx
    transfer_payload = {
        "from": "eosio", 
        "to": account, 
        "quantity": '{:.4f} {}'.format(liquid, SYMBOL), 
        "memo": "transfer genesis balance to {}".format(account)
    }
    
    transfer_data = cleos.abi_json_to_bin('eosio.token', 'transfer', transfer_payload)
    transfer_action = {
        "account": "eosio.token", 
        "name": "transfer", 
        "authorization": [
            {
                "actor": "eosio", 
                "permission": "active"
            }], 
        "data": transfer_data['binargs']}

    return (newaccount_action, buyram_action, delegate_action, transfer_action)

def get_chain_params():
    return cleos.get_table('eosio', 'eosio', 'global')['rows'][0]

def set_chain_params(params):
    set_params_payload = {
        'params': params 
    }
    set_params_data = cleos.abi_json_to_bin('eosio', 'setparams', set_params_payload)
    set_params_action = {
        'account' : 'eosio',
        'name' : 'setparams',
        'authorization' : [
            {
                'actor' : 'eosio',
                'permission' : 'active'
            } ],
        'data' : set_params_data['binargs']
    }
    trx = {"actions": [set_params_action]}
    return cleos.push_transaction(trx, KEY, broadcast=True)

def main():
    try:
        telos_genesis = pandas.read_csv(SNAPSHOT_FILE, dtype=str, names=['eth_address',
                                                                         'eos_account', 'eos_key', 'balance'])
    except Exception as e:
        logger.critical(
            'Error loading snapshot at {}: {}'.format(SNAPSHOT_FILE, e))
        exit(1)

    logging.info('Creating accounts')
    created_accounts = 0
    num_accounts = len(telos_genesis.index)

    #Set chain params to max performance
    logger.info('Setting chain params to max performance')
    global_params = get_chain_params()
    max_block_cpu_usage  = global_params['max_block_cpu_usage']
    max_transaction_cpu_usage = global_params['max_transaction_cpu_usage']
    global_params['max_block_cpu_usage'] = 100000000
    global_params['max_transaction_cpu_usage'] = 99999899

    try:     
        set_chain_params(global_params)
    except Exception as e:
        logger.critical('Error setting chain params: {}'.format(e))
        quit()
    
    #Create accounts
    for i in chunker(telos_genesis, BATCH_SIZE):
        actions = []
        for _, row in i.iterrows():
            actions.extend(get_account_creation_actions(
                row['eos_account'], row['balance'], row['eos_key']))
        #print(actions)
        trx = {"actions": actions}
        try: 
            resp = cleos.push_transaction(trx, KEY, broadcast=True)
        except Exception as e:
            logger.critical('Error creating accounts: {}'.format(e))
            quit()

        #logger.info(resp)
        created_accounts += BATCH_SIZE
        logger.info('Created {} accounts of {}'.format(created_accounts, num_accounts))

    #Setting back chain params to original values
    logger.info('Setting back chain params to original values')
    global_params['max_block_cpu_usage'] = max_block_cpu_usage
    global_params['max_transaction_cpu_usage'] = max_transaction_cpu_usage
    try:     
        set_chain_params(global_params)
    except Exception as e:
        logger.critical('Error setting back chain params: {}'.format(e))
        quit()

    logger.info('Injection finished')
if __name__ == "__main__":
    main()
