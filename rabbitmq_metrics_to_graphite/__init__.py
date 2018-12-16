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
        "Processing RabbitMQ: {} on graphite {}".format(rabbitmq["cluster_name"], graphite["host"]))
    starttime = time.time()
    sock = _socket_for_host_port(graphite["host"], graphite["port"])
    overview = rabbitClient.get_overview()
    # Object counts
    for m_instance in \
            ['channels', 'connections', 'consumers', 'exchanges', 'queues']:
        if m_instance in overview['object_totals']:
            _send_graphite_metric(
                sock, graphite, rabbitmq, '{}_total_count'.format(m_instance), overview['object_totals'][m_instance])

    # Aggregated Queue message stats
    for m_instance in \
            ['messages', 'messages_ready', 'messages_unacknowledged']:
        if m_instance in overview['queue_totals']:
            _send_graphite_metric(sock, graphite, rabbitmq, 'queue_{}_total_count'.format(
                m_instance), overview['queue_totals'][m_instance])

            _send_graphite_metric(sock, graphite, rabbitmq, 'queue_{}_total_rate'.format(m_instance), overview['queue_totals']['{}_details'.format(
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
            _send_graphite_metric(sock, graphite, rabbitmq, 'message_{}_total_count'.format(
                m_instance), overview['message_stats'][m_instance])

            _send_graphite_metric(sock, graphite, rabbitmq, 'message_{}_total_rate'.format(m_instance), overview['message_stats']['{}_details'.format(m_instance)]
                                  ['rate'])

    # Connections metrics
    connections = rabbitClient.get_connections()
    connections_dict = {'channels': 'channels',
                        'recv_oct': 'received_bytes',
                        'recv_cnt': 'received_packets',
                        'send_oct': 'send_bytes',
                        'send_cnt': 'send_packets',
                        'send_pend': 'send_pending'
                        }
    for m_instance, m_instance_label in connections_dict.iteritems():
        if connections is None:
            _send_graphite_metric(
                sock, graphite, rabbitmq, 'connections.{}'.format(m_instance_label), 0)
        elif m_instance in connections:
            _send_graphite_metric(sock, graphite, rabbitmq,
                                  'connections.{}'.format(m_instance_label), connections[m_instance])

    # Node metrics
    nodes = rabbitClient.get_nodes()
    nodes_dict = {'running': 'node_running',
                  'mem_used': 'node_mem_used',
                  'mem_limit': 'node_mem_limit',
                  'mem_alarm': 'node_mem_alarm',
                  'disk_free': 'node_disk_free',
                  'disk_free_alarm': 'node_disk_free_alarm',
                  'disk_free_limit': 'node_disk_free_limit',
                  'fd_used': 'node_file_descriptor_used',
                  'fd_total': 'node_file_descriptor_total',
                  'sockets_used': 'node_sockets_used',
                  'sockets_total': 'node_sockets_total',
                  'partitions_len': 'node_partitions'
                  }
    for m_instance, m_instance_label in nodes_dict.iteritems():
        if nodes is not None:
            for node in nodes:
                if m_instance in node:
                    # We multiply the metric value by 1 to handle boolean conversion
                    _send_graphite_metric(sock, graphite, rabbitmq,
                                          'nodes.{}.{}'.format(node['name'].replace('.', '_'), m_instance_label), node[m_instance]*1)

    # Queues
    queues = rabbitClient.get_queues()
    for m_instance in \
            ['messages_ready', 'messages_unacknowledged', 'messages',
             'messages_ready_ram', 'messages_unacknowledged_ram', 'messages_ram',
             'messages_persistent', 'message_bytes', 'message_bytes_ready',
             'message_bytes_unacknowledged', 'message_bytes_ram', 'message_bytes_persistent',
             'consumers', 'consumer_utilisation', 'memory', 'head_message_timestamp']:
        if queues is not None:
            for queue in queues:
                if m_instance in queue:
                    _send_graphite_metric(sock, graphite, rabbitmq,
                                          'queues.{}.{}'.format(queue['name'].replace('.', '_'), m_instance), queue[m_instance])

    timediff = time.time() - starttime
    # Send time elapsed for scrapping metrics
    _send_graphite_metric(sock, graphite, rabbitmq,
                          'scraptime_elapsed_seconds', timediff)

    sock.close()

    logging.info('All metrics has been sent from RabbitMQ [{}] to Graphite [{}] in: {} sec'.format(
        rabbitmq["cluster_name"], graphite["host"], round(timediff, 2)))


def _send_graphite_metric(sock, graphite, rabbitmq, metricName, metricValue):
    now = time.time()
    metric = '{0}.{1}.{2} {3} {4}\n'.format(
        graphite["prefix"], rabbitmq["cluster_name"], metricName, metricValue, now)
    logging.debug("Sending metric: {}".format(metric))
    sock.sendall(metric)


def _socket_for_host_port(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((host, port))
    sock.settimeout(None)
    return sock


def main():

    global rabbitClient
    # load config file
    configFilePath = args.config
    if os.path.isfile(configFilePath):
        logging.debug('Processing config file {}'.format(configFilePath))
        with open(configFilePath) as configFile:
            conf = json.load(configFile)
            logging.debug('Graphite configuration: {}'.format(
                conf["graphite_servers"]))
            logging.debug('RabbitMQ configuration: {}'.format(
                conf["rabbitmq_clusters"]))
            for rabbitmq in conf["rabbitmq_clusters"]:
                logging.debug(
                    'Working on Rabbitmq cluster: {}'.format(rabbitmq['cluster_name']))
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
    main()
