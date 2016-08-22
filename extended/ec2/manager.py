#!/usr/bin/env python

# Author: Omid Mashayekhi <omidm@stanford.edu>

import sys
import os
import time
import subprocess
import argparse

import ec2
import utils
import config


parser = argparse.ArgumentParser(description='Spark EC2 Manager.')
parser.add_argument(
    "-l", "--launch",
    dest="launch",
    action="store_true",
    help="launch ec2 instances")
parser.add_argument(
    "-t", "--terminate",
    dest="terminate",
    action="store_true",
    help="terminate ec2 instances")
parser.add_argument(
    "-m", "--monitor",
    dest="monitor",
    action="store_true",
    help="monitor ec2 instances")
parser.add_argument(
    "-s", "--start",
    dest="start",
    action="store_true",
    help="start the experiment")
parser.add_argument(
    "-e", "--end",
    dest="end",
    action="store_true",
    help="end the experiment")
parser.add_argument(
    "-d", "--download",
    dest="download",
    action="store_true",
    help="download the logs")
parser.add_argument(
    "-c", "--clean",
    dest="clean",
    action="store_true",
    help="clean the logs")
parser.add_argument(
    "-p", "--print",
    dest="printdns",
    action="store_true",
    help="print controller and slaves dns")
parser.add_argument(
    "-w", "--wake_up",
    dest="wakeup",
    action="store_true",
    help="ssh test in to nodes for testing")
parser.add_argument(
    "-o", "--only_submit",
    dest="onlysubmit",
    action="store_true",
    help="only submit the application to master, master and workers are already up")

parser.add_argument(
    "-mdns", "--master_dns",
    dest="masterdns",
    default="X-X-X-X",
    help="master dns")
parser.add_argument(
    "-mpdns", "--master_private_dns",
    dest="masterprivatedns",
    default="X-X-X-X",
    help="master private dns")
parser.add_argument(
    "-pdns", "--use_private",
    dest="useprivate",
    action="store_true",
    help="if specified will use the private dns for inter node communications")

args = parser.parse_args()


if (args.monitor):
  ec2.wait_for_instances_to_start(
      config.EC2_LOCATION,
      config.MASTER_NUM + config.SLAVE_NUM,
      placement_group=config.PLACEMENT_GROUP);

elif (args.launch):
  ans = raw_input("Are you sure you want to launch {} ec2 instances? (Enter 'yes' to proceed): ".format(config.MASTER_NUM + config.SLAVE_NUM))
  if (ans != 'yes'):
    print "Aborted"
    exit(0)

  print "Launching the instances ..."
  ec2.run_instances(
      config.EC2_LOCATION,
      config.SPARK_AMI,
      config.SLAVE_NUM,
      config.KEY_NAME,
      config.SECURITY_GROUP,
      config.PLACEMENT,
      config.PLACEMENT_GROUP,
      config.SLAVE_INSTANCE_TYPE);
  ec2.run_instances(
      config.EC2_LOCATION,
      config.SPARK_AMI,
      config.MASTER_NUM,
      config.KEY_NAME,
      config.SECURITY_GROUP,
      config.PLACEMENT,
      config.PLACEMENT_GROUP,
      config.MASTER_INSTANCE_TYPE);

elif (args.terminate):
  ans = raw_input("Are you sure you want to terminate all ec2 instances? (Enter 'yes' to proceed): ")
  if (ans != 'yes'):
    print "Aborted"
    exit(0)

  print "Terminating the instances ..."
  ec2.terminate_instances(
      config.EC2_LOCATION,
      placement_group=config.PLACEMENT_GROUP);

elif (args.start or args.end or args.download or args.clean or args.printdns or args.wakeup or args.onlysubmit):

  dns_names = ec2.get_dns_names(
      config.EC2_LOCATION,
      placement_group=config.PLACEMENT_GROUP);

  if (not args.masterdns == "X-X-X-X"):
    master_dns   = args.masterdns
    master_p_dns = args.masterprivatedns
  else:
    mdnss = ec2.get_dns_names(
        config.EC2_LOCATION,
        placement_group=config.PLACEMENT_GROUP,
        instance_type=config.MASTER_INSTANCE_TYPE);
    master_dns   = mdnss["public"][0]
    master_p_dns = mdnss["private"][0]
  
  slave_dnss = list(dns_names["public"])
  slave_dnss.remove(master_dns)
  
  if (not args.useprivate):
    master_p_dns = master_dns
    slave_p_dnss = list(slave_dnss)
  else:
    assert(not master_p_dns == "X-X-X-X") 
    slave_p_dnss = list(dns_names["private"])
    slave_p_dnss.remove(master_p_dns)

  if (args.printdns):
    print "Master DNS:         " + master_dns
    print "Master Private DNS: " + master_p_dns
    print "Slave DNSs:           " + str(slave_dnss)
    print "Slave Private DNSs:   " + str(slave_p_dnss)
  
  if (args.wakeup):
    utils.test_nodes(slave_dnss + [master_dns])
   
  if (args.start):
    utils.start_experiment(master_dns, master_p_dns, slave_dnss)
  
  if(args.download):
    utils.collect_logs(master_dns, slave_dnss)
  
  if (args.end):
    utils.stop_experiment(master_dns, slave_dnss)

  if (args.clean):
    utils.clean_logs(master_dns, slave_dnss)
  
  if (args.onlysubmit):
    utils.submit_application(master_dns, master_p_dns)
  
else :
  print "\n** Provide an action to perform!\n"
  print parser.print_help();


