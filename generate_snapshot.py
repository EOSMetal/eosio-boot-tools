#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import requests
import inspect
import hashlib
import pandas

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

def download_file(filename, url):
    with open(filename, 'wb') as fout:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        # Write response data to file
        for block in response.iter_content(4096):
            fout.write(block)


def sha256sum(filename):
    h = hashlib.sha256()
    with open(filename, 'rb', buffering=0) as f:
        for b in iter(lambda: f.read(128*1024), b''):
            h.update(b)
    return h.hexdigest()


def main():
    EOS_GENESIS_HASH = '6df61f12f96f89c907fac14a021d788c9e77098952a6c5494c7999d2e79d0a35'
    EOS_GENESIS_FILE = SCRIPT_PATH + '/snapshot.csv'
    TELOS_GENESIS_FILE = SCRIPT_PATH + '/eosmetal_telos_snapshot.csv'
    EOS_GENESIS_BALANCE = 996690678.8328998
    TELOS_GENESIS_BALANCE = 178473249.3125

    # Get EOS genesis file from EOS Authority and check the hash
    if not os.path.exists(EOS_GENESIS_FILE):
        logger.info('Downloading EOS genesis')
        download_file(SCRIPT_PATH + '/snapshot.csv',
                      'https://raw.githubusercontent.com/eoscafe/eos-snapshot-validation/master/eosnewyork/snapshot.csv')

    eos_genesis_checksum = sha256sum(EOS_GENESIS_FILE)
    logger.debug('EOS genesis checksum: {}'.format(eos_genesis_checksum))
    if eos_genesis_checksum == EOS_GENESIS_HASH:
        logger.info('EOS genesis checksum OK')
    else:
        logger.critical('EOS genesis checksum failed')
        exit(1)

    # Apply the cap and check the balances
    eos_genesis = pandas.read_csv(EOS_GENESIS_FILE, names=['eth_address',
                                                           'eos_account', 'eos_address', 'balance'])

    eos_total_balance = eos_genesis['balance'].sum()
    logger.debug(
        'EOS genesis total balance: {} EOS'.format(eos_total_balance))
    if eos_total_balance != EOS_GENESIS_BALANCE:
        logger.critical('EOS genesis balance is wrong')
        exit(1)
    else:
        logger.info('EOS genesis balance correct')

    eos_genesis['balance'] = eos_genesis['balance'].apply(
        lambda x: x if x < 40000.0 else 40000.0)
    telos_total_balance = eos_genesis['balance'].sum()
    logger.debug('TELOS genesis total balance: {} TLOS'.format(
        telos_total_balance))
    eos_genesis.to_csv(TELOS_GENESIS_FILE, header=False, float_format='%.4f')
    if telos_total_balance != TELOS_GENESIS_BALANCE:
        logger.critical('TELOS genesis balance is wrong')
        exit(1)
    else:
        logger.info('TELOS genesis balance correct')


if __name__ == "__main__":
    main()
