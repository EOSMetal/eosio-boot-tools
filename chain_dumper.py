#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import inspect
import zmq
import json
import pprint

SCRIPT_PATH = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
    
parser = argparse.ArgumentParser()
parser.add_argument("-v", '--verbose', action="store_true",
                    dest="verbose", help='Print logged info to screen')
parser.add_argument("-d", '--debug', action="store_true",
                    dest="debug", help='Print debug info')
parser.add_argument('-l', '--log_file', default='{}.log'.format(
    os.path.basename(__file__).split('.')[0]), help='Log file')
parser.add_argument('-o', '--dump_file', default='{}/{}'.format(SCRIPT_PATH, 'chain_dump.txt'), help='Log file')
parser.add_argument('-b', '--block_num', type=int, required=True,
                    help='Block number to stop the dump(included)')
parser.add_argument('-z', '--zmq_socket',
                    default='tcp://127.0.0.1:5556', help='ZMQ socket where to listen')
args = parser.parse_args()

VERBOSE = args.verbose
DEBUG = args.debug
LOG_FILE = args.log_file
DUMP_FILE = args.dump_file
BLOCK_NUM = args.block_num
ZMQ_SOCKET = args.zmq_socket

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
pp = pprint.PrettyPrinter(indent=2)

def main():
    context = zmq.Context()
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect(ZMQ_SOCKET)
    

    if os.path.exists(DUMP_FILE):
      os.remove(DUMP_FILE)

    logger.info('Getting accounts from chain')
    logger.info('Saving accounts to {}'.format(DUMP_FILE))
    file = open(DUMP_FILE, "a+")
    while True:
        data = consumer_receiver.recv()
        action =  json.loads(data[8:])
        code = data[0:8]
        try:
          if 'action_trace' in action:
            if action['action_trace']['act']['name'] == 'newaccount':
              block_num = action['action_trace']['block_num']
              if block_num > BLOCK_NUM:
                logger.info('Dump finished')
                quit()
              else:
                file.write('{}\n'.format(action['action_trace']['act']['data']['name']))
                file.flush()

        except Exception as e:
          logger.critical('Error dumping accounts')
          logger.critical(action)
          logger.critical(e)
          quit()


if __name__ == "__main__":
    main()

