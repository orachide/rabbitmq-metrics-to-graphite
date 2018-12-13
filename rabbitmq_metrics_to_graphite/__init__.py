#!/usr/bin/env python

import logging
import json
import socket
import time
import sys
import os
from pyrabbit.http import HTTPError
from pyrabbit.api import Client
import argparse
parser = argparse.ArgumentParser(
    description="Retrieve RabbitMQ metrics and send them to graphite")
parser.add_argument("config", help="path to the config file",
                    type=str)
parser.add_argument("-v", "--verbose", action="store_true",
                    help="increase output verbosity")
args = parser.parse_args()
#logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s')
if args.verbose:
    logging.basicConfig(
        format='%(asctime)s\t%(levelname)s:\t%(message)s', level=logging.DEBUG)
else:
    logging.basicConfig(
        format='%(asctime)s\t%(levelname)s:\t%(message)s', level=logging.INFO)


def process(rabbitmq, graphite):
    logging.debug(
        "Processing RabbitMQ: {} on graphite {}".format(rabbitmq, graphite))
    starttime = time.time()
    sock = _socket_for_host_port(graphite["host"], graphite["port"])
    overview = rabbitClient.get_overview()
    # Object counts
    for m_instance in \
            ['channels', 'connections', 'consumers', 'exchanges', 'queues']:
        if m_instance in overview['object_totals']:
            _send_graphite_metric(
                sock, graphite['prefix'], m_instance, overview['object_totals'][m_instance])

    # Aggregated Queue message stats
    for m_instance in \
            ['messages', 'messages_ready', 'messages_unacknowledged']:
        if m_instance in overview['queue_totals']:
            _send_graphite_metric(sock, graphite['prefix'], 'queue_total-{}-count'.format(
                m_instance), overview['queue_totals'][m_instance])

            _send_graphite_metric(sock, graphite['prefix'], 'queue_total-{}-rate'.format(m_instance), overview['queue_totals']['{}_details'.format(
                m_instance)]
                ['rate'])

    # Aggregated Message Stats
    for m_instance in \
            [
                'ack', 'confirm', 'deliver', 'deliver_get', 'deliver_no_ack',
                'get', 'get_no_ack', 'publish', 'publish_in', 'publish_out',
                'redeliver', 'return_unroutable'
            ]:
        if m_instance in overview['message_stats']:
            _send_graphite_metric(sock, graphite['prefix'], 'message_total-{}-count'.format(
                m_instance), overview['message_stats'][m_instance])

            _send_graphite_metric(sock, graphite['prefix'], 'message_total-{}-rate'.format(m_instance), overview['message_stats']['{}_details'.format(m_instance)]
                                  ['rate'])

    # Configurable per-queue message counts
    for queue_name in rabbitmq["queues"]:
        messages_detail = None
        try:
            messages_detail = rabbitClient.get_messages(
                rabbitmq["vhost"], queue_name, 1, True)
        except HTTPError as err:
            logging.error(
                'Error Opening Queue [{}] details: {}'
                .format(queue_name, err))
        if messages_detail is None:
            count = 0
        else:
            # the consume message is not counted
            count = messages_detail[0]['message_count'] + 1
        _send_graphite_metric(
            sock, graphite['prefix'], 'msg_count-{}'.format(queue_name), count)

    sock.close()

    timediff = time.time() - starttime
    logging.info('All metrics sent in: {} sec'.format(round(timediff, 2)))


def _send_graphite_metric(sock, graphitePrefix, metricName, metricValue):
    now = time.time()
    metric = '{0}.{1} {2} {3}\n'.format(
        graphitePrefix, metricName, metricValue, now)
    logging.debug("Sending metric: {}".format(metric))
    sock.sendall(metric)


def _socket_for_host_port(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((host, port))
    sock.settimeout(None)
    return sock


def main(configFilePath):

    global rabbitClient
    # load config file
    if os.path.isfile(configFilePath):
        logging.debug('Processing config file {}'.format(configFilePath))
        with open(configFilePath) as configFile:
            conf = json.load(configFile)
            logging.debug('Graphite configuration: {}'.format(
                conf["graphite_servers"]))
            logging.debug('RabbitMQ configuration: {}'.format(
                conf["rabbitmq_servers"]))
            for rabbitmq in conf["rabbitmq_servers"]:
                logging.debug(
                    'Working on rabbitmq server: {}'.format(rabbitmq['host']))
                rabbitClient = Client('{}:{}'.format(
                    rabbitmq['host'], rabbitmq['port']), rabbitmq['username'], rabbitmq['password'])
                for graphite in conf["graphite_servers"]:
                    process(rabbitmq, graphite)
    else:
        logging.error('You must pass existing configFilePath, actual is {}'.format(
            configFilePath))
        sys.exit(1)


def excepthook(type, value, tb):
    # you can log the exception to a file here
    logging.error('ERROR: {} {}'.format(type, value))

    # the following line does the default (prints it to err)
    sys.__excepthook__(type, value, tb)


sys.excepthook = excepthook

if __name__ == '__main__':
    main(args.config)
