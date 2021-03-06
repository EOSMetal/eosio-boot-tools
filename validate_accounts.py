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
import csv
import requests
import re
import traceback
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
BP_ACCOUNTS_FILE = 'initial_block_producers.csv'
EOS_BP_ACCOUNTS_FILE = 'eos_bp_accounts.csv'
RAM_ACCOUNTS_FILE = 'ram_accounts.csv'
TCRP_ACCOUNTS_FILE = 'tcrp_accounts.csv'
TFRP_ACCOUNTS_FILE = 'tfrp_accounts.csv'
TFVT_ACCOUNTS_FILE = 'tfvt_accounts.csv'
SPECIAL_ACCOUNTS_FILE = 'special_accounts.csv'
SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
cleos = eospy.cleos.Cleos(url=API_ENDPOINT)

def asset2float(asset):
    return float(asset.split(' ')[0])

def get_account_info(account):
    try:
        result = cleos.get_account(account)
        
        if not  'core_liquid_balance' in result:
            result['core_liquid_balance'] = "0"
        balance = round(asset2float(result['core_liquid_balance']) + asset2float(result['total_resources']['cpu_weight']) + asset2float(result['total_resources']['net_weight']), 4)
        
        if len(result['permissions'][0]['required_auth']['accounts']) == 0:
            key = result['permissions'][0]['required_auth']['keys'][0]['key']
            if len(result['permissions']) > 2:
                logger.critical('Account {} has more than 2 permissions'.format(account))
                return '', 0
            if result['permissions'][0]['required_auth']['keys'][0]['key'] != result['permissions'][0]['required_auth']['keys'][0]['key']:
                logger.critical('Owner and Active keys for account {} are different'.format(account))
                return '', 0
            if len(result['permissions'][0]['required_auth']['keys']) > 1 or len(result['permissions'][1]['required_auth']['keys']) > 1 or len(result['permissions'][0]['required_auth']['accounts']) > 0 or len(result['permissions'][1]['required_auth']['accounts']) > 0:
                logger.critical('Account {} has weird accounts or keys'.format(account))
                return '', 0
        else:
            key = ''

    except Exception as e:
        if 'unknown key' in str(e):
            logger.critical('Account {} is not on chain'.format(account))
        else:
            logger.critical(e)
        return '', 0

    #logger.debug('{} {} {}'.format(account, key, balance))
    return key, balance

def download_file(filename, url):
    with open(filename, 'wb') as fout:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        # Write response data to file
        for block in response.iter_content(4096):
            fout.write(block)

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

def load_csv(file):
    with open(file, newline='') as csvfile:
        data = list(csv.reader(csvfile))
    return data

def main():
    #Check bp accounts
    download_file(BP_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/initial_block_producers.csv')
    logger.info('Loading bp_accounts...')
    try:
        with open(BP_ACCOUNTS_FILE, 'r') as fin:
            data = fin.read().splitlines(True)
        with open(BP_ACCOUNTS_FILE, 'w') as fout:
            fout.writelines(data[1:])
        
        bp_accounts = pd.read_csv(BP_ACCOUNTS_FILE, dtype=str, names=['a', 'b', 'eos_account', 'eos_key', 'balance', 'd']).drop(columns=['a', 'b', 'd', 'balance']).sort_values(by=['eos_account'])

        #Remove tabs
        bp_accounts['eos_account'] = bp_accounts['eos_account'].apply(lambda x: re.sub(r"[\n\t\s]*", "", x))                                                                         
        bp_accounts['eos_key'] = bp_accounts['eos_key'].apply(lambda x: re.sub(r"[\n\t\s]*", "", x))                  
        #bp_accounts['balance'] = bp_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))                                                                 
        bp_accounts = bp_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading bp accounts snapshot at {}: {}'.format(BP_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting bp accounts from chain...')
    try:
        chain_accounts = get_accounts(bp_accounts['eos_account'].tolist()).drop(columns=['balance']).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting bp acounts from chain: {}'.format(e))
        quit()
    
    logger.info('Checking bp accounts...')
    if bp_accounts.equals(chain_accounts):
        logger.info('All bp accounts are present on chain with the right key')
    else:
        ne_stacked = (bp_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(bp_accounts != chain_accounts)
        changed_from = bp_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('BP accounts in csv and chain don`t match')
        print(changes)
    
    #Check eos bp accounts
    download_file(EOS_BP_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/eos_bp_accounts.csv')
    logger.info('Loading eos_bp_accounts...')
    try:
        with open(EOS_BP_ACCOUNTS_FILE, 'r') as fin:
            data = fin.read().splitlines(True)
        with open(EOS_BP_ACCOUNTS_FILE, 'w') as fout:
            fout.writelines(data[1:])
        
        bp_accounts = pd.read_csv(EOS_BP_ACCOUNTS_FILE, dtype=str, names=['a', 'eos_account', 'eos_key', 'balance']).drop(columns=['a', 'balance']).sort_values(by=['eos_account'])

        #Remove tabs
        bp_accounts['eos_account'] = bp_accounts['eos_account'].apply(lambda x: re.sub(r"[\n\t\s]*", "", x))                                                                         
        bp_accounts['eos_key'] = bp_accounts['eos_key'].apply(lambda x: re.sub(r"[\n\t\s]*", "", x))                  
        #bp_accounts['balance'] = bp_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))                                                                 
        bp_accounts = bp_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading bp accounts snapshot at {}: {}'.format(EOS_BP_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting eos bp accounts from chain...')
    try:
        chain_accounts = get_accounts(bp_accounts['eos_account'].tolist()).drop(columns=['balance']).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting eos bp acounts from chain: {}'.format(e))
        quit()
    
    logger.info('Checking eos bp accounts...')

    if bp_accounts.equals(chain_accounts):
        logger.info('All eos bp accounts are present on chain with the right key')
    else:
        ne_stacked = (bp_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(bp_accounts != chain_accounts)
        changed_from = bp_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('EOS BP accounts in csv and chain don`t match')
        print(changes)
    
    #Check ram accounts
    download_file(RAM_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/ram_accounts.csv')
    logger.info('Loading ram_accounts...')
    try:
        ram_accounts = pd.read_csv(RAM_ACCOUNTS_FILE, dtype=str, names=['eth_address','unknown',
                                                                         'eos_account', 'eos_key', 'balance']).drop(columns=['eth_address', 'balance']).drop(columns=['unknown']).sort_values(by=['eos_account'])
        ram_accounts = ram_accounts.drop(0)
        #ram_accounts['balance'] = ram_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))           
        ram_accounts = ram_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading ram accounts snapshot at {}: {}'.format(TCRP_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting ram accounts from chain...')
    try:
        chain_accounts = get_accounts(ram_accounts['eos_account'].tolist()).drop(columns=['balance']).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting ram acounts from chain: {}'.format(e))
    
    logger.info('Checking ram accounts...')
    if ram_accounts.equals(chain_accounts):
        logger.info('All ram accounts are present on chain with the right key')
    else:
        ne_stacked = (ram_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(ram_accounts != chain_accounts)
        changed_from = ram_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('ram accounts in csv and chain don`t match')
        print(changes)
    
    #Check tcrp accounts
    download_file(TCRP_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/tcrp_accounts.csv')
    logger.info('Loading tcrp_accounts...')
    try:
        tcrp_accounts = pd.read_csv(TCRP_ACCOUNTS_FILE, dtype=str, names=[
                                                                         'eos_account', 'eos_key', 'balance']).drop(columns=['balance']).sort_values(by=['eos_account'])
        tcrp_accounts = tcrp_accounts.drop(0)
        #tcrp_accounts['balance'] = tcrp_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))           
        tcrp_accounts = tcrp_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading tcrp accounts snapshot at {}: {}'.format(TCRP_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting tcrp accounts from chain...')
    try:
        chain_accounts = get_accounts(tcrp_accounts['eos_account'].tolist()).drop(columns=['balance']).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting tcrp acounts from chain: {}'.format(e))
    
    logger.info('Checking tcrp accounts...')
    if tcrp_accounts.equals(chain_accounts):
        logger.info('All tcrp accounts are present on chain with the right key')
    else:
        ne_stacked = (tcrp_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(tcrp_accounts != chain_accounts)
        changed_from = tcrp_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('tcrp accounts in csv and chain don`t match')
        print(changes)
    
    #Check tfrp accounts
    download_file(TFRP_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/tfrp_accounts.csv')
    logger.info('Loading tfrp_accounts...')
    with open(TFRP_ACCOUNTS_FILE, 'r') as fin:
        data = fin.read().splitlines(True)
    with open(TFRP_ACCOUNTS_FILE, 'w') as fout:
        fout.writelines(data[1:])
    try:
        tfrp_accounts = pd.read_csv(TFRP_ACCOUNTS_FILE, dtype=str, names=[
                                                                         'eos_account', 'eos_key', 'balance']).sort_values(by=['eos_account'])
        #tfrp_accounts = tfrp_accounts.drop(0)
        tfrp_accounts['balance'] = tfrp_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))           
        tfrp_accounts = tfrp_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading tfrp accounts snapshot at {}: {}'.format(TFRP_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting tfrp accounts from chain...')
    try:
        chain_accounts = get_accounts(tfrp_accounts['eos_account'].tolist()).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting tfrp acounts from chain: {}'.format(e))
    
    logger.info('Checking tfrp accounts...')
    if tfrp_accounts.equals(chain_accounts):
        logger.info('All tfrp accounts are present on chain with the right key')
    else:
        ne_stacked = (tfrp_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(tfrp_accounts != chain_accounts)
        changed_from = tfrp_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('tfrp accounts in csv and chain don`t match')
        print(changes)

    #Check tfvt accounts
    download_file(TFVT_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/tfvt_accounts.csv')
    logger.info('Loading tfvt_accounts...')
    try:
        tfvt_accounts = pd.read_csv(TFVT_ACCOUNTS_FILE, dtype=str, names=['a', 'b',
                                                                         'eos_account', 'eos_key', 'balance']).drop(columns=['a', 'b', 'balance']).sort_values(by=['eos_account'])
        tfvt_accounts = tfvt_accounts.drop(0)
        #tfvt_accounts['balance'] = tfvt_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))           
        tfvt_accounts = tfvt_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading tfvt accounts snapshot at {}: {}'.format(TFVT_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting tfvt accounts from chain...')
    try:
        chain_accounts = get_accounts(tfvt_accounts['eos_account'].tolist()).drop(columns=['balance']).sort_values(by=['eos_account'])
    except Exception as e:
        logger.critical('Error getting tfvt acounts from chain: {}'.format(e))
    
    logger.info('Checking tfvt accounts...')
    if tfvt_accounts.equals(chain_accounts):
        logger.info('All tfvt accounts are present on chain with the right key')
    else:
        ne_stacked = (tfvt_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(tfvt_accounts != chain_accounts)
        changed_from = tfvt_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('tfvt accounts in csv and chain don`t match')
        print(changes)

    #Check special accounts
    download_file(SPECIAL_ACCOUNTS_FILE,'https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/telos_special_accounts.csv')
    logger.info('Loading special_accounts...')
    try:
        special_accounts = pd.read_csv(SPECIAL_ACCOUNTS_FILE, dtype=str, names=['eth_address',
                                                                         'eos_account', 'eos_key', 'balance']).drop(columns=['eth_address']).drop(columns=['eos_key']).drop(columns=['balance']).sort_values(by=['eos_account'])
        special_accounts = special_accounts.drop(0)
        #special_accounts['balance'] = special_accounts['balance'].apply(lambda x: '{:.4f}'.format(float(x)))           
        special_accounts = special_accounts.reset_index(drop=True) 
    except Exception as e:
        logger.critical(
            'Error loading special accounts snapshot at {}: {}'.format(SPECIAL_ACCOUNTS_FILE, e))
        exit(1)

    logger.info('Getting special accounts from chain...')
    try:
        chain_accounts = get_accounts(special_accounts['eos_account'].tolist()).sort_values(by=['eos_account']).drop(columns=['eos_key']).drop(columns=['balance'])
    except Exception as e:
        logger.critical('Error getting tcrp acounts from chain: {}'.format(e))
    
    logger.info('Checking special accounts...')
    if special_accounts.equals(chain_accounts):
        logger.info('All special accounts are present on chain') #with the right key and balance')
    else:
        ne_stacked = (special_accounts != chain_accounts).stack()
        changed = ne_stacked[ne_stacked]
        difference_locations = np.where(special_accounts != chain_accounts)
        changed_from = special_accounts.values[difference_locations]
        changed_to = chain_accounts.values[difference_locations]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        logger.critical('special accounts in csv and chain don`t match')
        print(changes)

    #Check genesis accounts
    download_file('key_recovery.csv','https://raw.githubusercontent.com/Telos-Foundation/snapshots/master/key_recovery.csv')
    key_recovery = load_csv('key_recovery.csv')

    logger.info('Loading snapshot...')
    try:
        telos_genesis = pd.read_csv(SNAPSHOT_FILE, dtype=str, names=['eth_address',
                                                                         'eos_account', 'eos_key', 'balance']).sort_values(by=['eos_account'])
        
        logger.info('Merging key recovery...')
        for row in key_recovery:
            telos_genesis.loc[telos_genesis['eth_address'] == row[0].lower(), ['eos_key']] = row[1]

        telos_genesis =telos_genesis.drop(columns=['eth_address'])
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
