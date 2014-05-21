#! /usr/bin/python
"""Small script to read lines from stdin and publish them to AMQP.

   Mostly a proof-of-concept for the new rsyslog feature of external output
   modules (http://de.slideshare.net/rainergerhards1/writing-rsyslog-p).
   For rsyslog use omrabbitmq instead.

   Copyright (C) 2014 by Martin Schuette, DECK36 GmbH & Co KG

   Based on the skeleton for a python rsyslog output plugin
   Copyright (C) 2014 by Adiscon GmbH

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0
         -or-
         see COPYING.ASL20 in the source distribution

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import sys, os
import select
import argparse
import logging

import pika
import json

# skeleton config parameters
pollPeriod = 0.75 # the number of seconds between polling for new messages
maxAtOnce = 1024  # max nbr of messages that are processed within one batch

# App logic global variables
args = {}
logger = {}

def setup():
	global args, logger
	parser = argparse.ArgumentParser(description='forward logs from stdin to AMQP')
	parser.add_argument('-d', '--debug',    help="print debug messages to logfile",     action='store_true')
	parser.add_argument('-v', '--verbose',  help="print some info messages to logfile", action='store_true')
	parser.add_argument('-l', '--logfile',  help="logfile for verbose and debug output (default: /tmp/om_amqp.py.log)", default='/tmp/om_amqp.py.log')
	parser.add_argument('-f', '--format',   help="input format, either msg or json (default), currently only used for content-type",  default='json')
	parser.add_argument('-u', '--user',     help="AMQP user (default: syslog)",         default='syslog')
	parser.add_argument('-p', '--password', help="AMQP password (default: syslog)",     default='syslog')
	parser.add_argument('-V', '--vhost',    help="AMQP vhost (default: syslog)",        default='syslog')
	parser.add_argument('-q', '--queue',    help="AMQP queue (default: syslog)",        default='syslog')
	parser.add_argument('-e', '--exchange', help="AMQP exchange (default: syslog)",     default='syslog')
	parser.add_argument('-k', '--key',      help="AMQP routing key (default: syslog)",  default='syslog')
	parser.add_argument('-s', '--server',   help="AMQP server (default: amqphost)",     default='amqphost')
	parser.add_argument('-c', '--confirm',  help="enable AMQP confirmed delivery",      action='store_true')
	args = parser.parse_args()

	if args.format == 'json':
		args.mimetype = 'text/json'
	else:
		args.mimetype = 'text/plain'

	# create logger obj
	logger = logging.getLogger('om_amqp')
	logger.propagate = False
	formatter = logging.Formatter('%(asctime)s - %(name)s/{} - %(levelname)s - %(message)s'.format(os.getpid()))
	fh = logging.FileHandler(args.logfile)
	fh.setFormatter(formatter)
	logger.addHandler(fh)

	# set log levels for debugging
	if args.debug:
		logger.setLevel(logging.DEBUG)
		args.verbose = True
	elif args.verbose:
		logger.setLevel(logging.INFO)
	else:
		logger.setLevel(logging.WARN)

def amqp_connect(user, pwd, srv, vhost, queue, exchange, confirm = False):
	credentials = pika.PlainCredentials(user, pwd)
	parameters = pika.ConnectionParameters(host=srv, virtual_host=vhost, credentials=credentials)
	try:
		connection = pika.BlockingConnection(parameters)
	except pika.exceptions.AMQPError, e:
		logging.warn("cannot connect to server {} vhost/queue {}/{} as user {}. Error: {}".format(srv, vhost, queue, user, e))
		sys.exit(1)
	channel = connection.channel()

	# ensure correct server config -- you might want to change this:
	channel.exchange_declare(exchange=exchange, durable=True, type="fanout")
	channel.queue_declare(queue=queue, durable=True)
	channel.queue_bind(exchange=exchange, queue=queue)
	
	if confirm:
		channel.confirm_delivery()
	logging.info("connected to server {} vhost/queue/exchange {}/{}/{} as user {}".format(srv, vhost, queue, exchange, user))
	return (connection, channel)

def onInit():
	""" Do everything that is needed to initialize processing (e.g.
	    open files, create handles, connect to systems...)
	"""
	global args, logger
	setup()
	(args.connection, args.channel) = amqp_connect(args.user, args.password, args.server, args.vhost, args.queue, args.exchange, args.confirm)
	logger.info("finished onInit(), established AMQP connection")

def onReceive(msgs):
	"""This is the entry point where actual work needs to be done. It receives
	   a list with all messages pulled from rsyslog. The list is of variable
	   length, but contains all messages that are currently available. It is
	   suggest NOT to use any further buffering, as we do not know when the
	   next message will arrive. It may be in a nanosecond from now, but it
	   may also be in three hours...
	"""
	global args, logger
	for msg in msgs:
		msg = msg.rstrip('\n')
		rc = args.channel.basic_publish(exchange=args.exchange,
					routing_key=args.key,
					body=msg,
					properties=pika.BasicProperties(content_type=args.mimetype, delivery_mode=1))
		logger.debug("onReceive() status {}, msg: {}".format(rc, repr(msg)))


def onExit():
	""" Do everything that is needed to finish processing (e.g.
	    close files, handles, disconnect from systems...). This is
	    being called immediately before exiting.
	"""
	global args, logger
	args.channel.close()
	logger.info("onExit(), closed AMQP connection")


"""
-------------------------------------------------------
This is plumbing that DOES NOT need to be CHANGED
-------------------------------------------------------
Implementor's note: Python seems to very agressively
buffer stdouot. The end result was that rsyslog does not
receive the script's messages in a timely manner (sometimes
even never, probably due to races). To prevent this, we
flush stdout after we have done processing. This is especially
important once we get to the point where the plugin does
two-way conversations with rsyslog. Do NOT change this!
See also: https://github.com/rsyslog/rsyslog/issues/22
"""
onInit()
keepRunning = 1
while keepRunning == 1:
	while keepRunning and sys.stdin in select.select([sys.stdin], [], [], pollPeriod)[0]:
		msgs = []
	        msgsInBatch = 0
		while keepRunning and sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
			line = sys.stdin.readline()
			if line:
				msgs.append(line)
			else: # an empty line means stdin has been closed
				keepRunning = 0
			msgsInBatch = msgsInBatch + 1
			if msgsInBatch >= maxAtOnce:
				break;
		if len(msgs) > 0:
			onReceive(msgs)
			sys.stdout.flush() # very important, Python buffers far too much!
onExit()
