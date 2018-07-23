#!/usr/bin/python

import subprocess
import os
import sys
import urllib2
from urllib2 import URLError
import socket
import re
import base64
import json
import time
import ConfigParser
from ConfigParser import SafeConfigParser
from pprint import pprint

def get_config_params(config_file):
  try:
    with open(config_file) as f:
      try:
        parser = SafeConfigParser()
        parser.readfp(f)
      except ConfigParser.Error, err:
        print 'Could not parse: %s ', err
        return False
  except IOError as e:
    print "Unable to access %s. Error %s \nExiting" % (config_file, e)
    sys.exit(1)

  ambari_server_host = parser.get('ambari_config', 'ambari_server_host')
  ambari_server_port = parser.get('ambari_config', 'ambari_server_port')
  ambari_user = parser.get('ambari_config', 'ambari_user')
  ambari_pass = parser.get('ambari_config', 'ambari_pass')
  ambari_server_timeout = parser.get('ambari_config', 'ambari_server_timeout')
  cluster_name = parser.get('ambari_config', 'cluster_name')

  if not ambari_server_port.isdigit():
    print "Invalid port specified for Ambari Server. Exiting"
    sys.exit(1)
  if not is_valid_hostname(ambari_server_host):
    print "Invalid hostname provided for Ambari Server. Exiting"
    sys.exit(1)
  if not ambari_server_timeout.isdigit():
    print "Invalid timeout value specified for Ambari Server. Using default of 30 seconds"
    ambari_server_timeout = 30

  # Prepare dictionary object with config variables populated
  config_dict = {}
  config_dict["ambari_server_host"] = ambari_server_host
  config_dict["ambari_server_port"] = ambari_server_port
  config_dict["ambari_server_timeout"] = ambari_server_timeout

  if re.match(r'^[A-Za-z0-9_]+$', cluster_name):
    config_dict["cluster_name"] = cluster_name
  else:
    print "Invalid Cluster name provided. Cluster name should have only alphanumeric characters and underscore. Exiting"
    return False

  if re.match(r'^[a-zA-Z0-9_.-]+$', ambari_user):
    config_dict["ambari_user"] = ambari_user
  else:
    print "Invalid Username provided. Exiting"
    return False

  config_dict["ambari_pass"] = ambari_pass

  return config_dict

def is_valid_hostname(hostname):
    if hostname == "":
        return False
    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1] # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

def test_socket(socket_host,socket_port,service_name):
  # Test socket connectivity to requested service port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((socket_host,int(socket_port)))
    except Exception as e:
      print("Unable to connect to %s host %s:%d. Exception is %s\nExiting!" % (service_name, socket_host,int(socket_port),e))
      sys.exit(1)
    finally:
      s.close()

# Submit new Capacity Scheduler config to from Ambari server
def submit_scheduler_config(scheduler_config,config_dict):
    ambari_server_host = str(config_dict['ambari_server_host'])
    ambari_server_port = str(int(config_dict['ambari_server_port']))
    ambari_server_timeout = float(config_dict['ambari_server_timeout'])
    cluster_name = str(config_dict['cluster_name'])
    ambari_user = config_dict['ambari_user']
    ambari_pass = config_dict['ambari_pass']

    # Test socket connectivity to Ambari server port
    test_socket(ambari_server_host,ambari_server_port,"Ambari server")

    # Construct URL request for metrics data. This needs to be changed when moving to JMX
    url = "http://"+ ambari_server_host +":" + ambari_server_port + "/api/v1/clusters/" + cluster_name
    auth_string = "%s:%s" % (ambari_user, ambari_pass)
    auth_encoded = 'Basic %s' % base64.b64encode(auth_string).strip()
    json_len = len(scheduler_config)
    req = urllib2.Request(url,data=scheduler_config)
    req.get_method = lambda: 'PUT'
    req.add_header('Content-Length', json_len )
    req.add_header('Content-Type','application/json')
    req.add_header('Accept','application/json')
    req.add_header('Authorization', auth_encoded)

    httpHandler = urllib2.HTTPHandler()
    httpHandler.set_http_debuglevel(1)
    opener = urllib2.build_opener(httpHandler)

    try:
      response = opener.open(req,timeout=ambari_server_timeout)
      print "Response code was: %d" %response.getcode()
    except (urllib2.URLError, urllib2.HTTPError) as e:
      print 'Scheduler config change request failed with error:', e
    except TypeError as e:
      print('Invalid format for submission data %s ' % e)

def validate_ambari_json(json_file):
    try:
        with open(json_file) as f:
          property_file = json.load(f)
          property_type = property_file['Clusters']['desired_config']['type']
          property_entries = property_file['Clusters']['desired_config']['properties']
          if (property_type == "capacity-scheduler"):
            for k,v in property_entries.iteritems():
              if not (k.startswith('yarn.scheduler.capacity.')):
                print "Capacity Scheduler entries should begin with yarn.scheduler.capacity.XXX. Please remove other entries unrelated to Capacity Scheduler"
                return False
            return property_file
          else:
            print "Incorrect syntax for Capacity Scheduler configuration json file. No type capacity_scheduler specified."
            return False
    except ValueError as e:
        print('Invalid json file provided. Error : %s' % e)
        return False

def main():

  config_file = os.path.join(os.path.dirname(__file__),"ambari_config.ini")
  ambari_config_dict = {}
  ambari_config_dict = get_config_params(config_file)

  scheduler_file = sys.argv[1] if len(sys.argv) >= 2 else os.path.join(os.path.dirname(__file__),"scheduler.json")
  scheduler_json = validate_ambari_json(scheduler_file)
  if(scheduler_json):
    submit_scheduler_config(json.dumps(scheduler_json),ambari_config_dict)
  else:
   print('File provided %s not a valid json'% scheduler_file)

if __name__ == "__main__":
  main()
