import json
import sys
import threading
import traceback
import os

import boto3
import paramiko
from scp import SCPClient

class ManageInstances:
  def __init__(self, num_instances, size=100):
    self.size = size
    self.num_instances = num_instances
    self.instance_ids = []
    self.result = []
    self.results_count = []
    self.lock = threading.Lock()
    self.ssh_lock = threading.Lock()
    self.client = boto3.client('ec2')
    self.ec2 = boto3.resource('ec2')
    self.get_instances()
    self.wait_for_checks()

  def get_instances(self):
    print("Getting instances")
    reservations = self.client.describe_instances()['Reservations']
    for reservation in reservations:
      for instance in reservation['Instances']:
        if instance['State']['Name']!='terminated':
          self.instance_ids.append(instance['InstanceId'])
    self.instance_ids = self.instance_ids[0:self.num_instances]
    # self.instance_ids = self.instance_ids[0:self.num_instances]
    print("Got instances")

  def wait_for_checks(self):
    print("Waiting for checks")
    self.client.start_instances(InstanceIds=self.instance_ids)
    waiter = self.client.get_waiter('instance_status_ok')
    waiter.wait()
    print("Checks completed")

  def create_file(self):
    print(f"result length: {len(self.result)}")
    with open('result.json', 'w+') as f:
      try:
        json.dump(self.result, f)
      except:
        print("Unable to dump data")

  def create_ssh_client(self, dns_name):
    print("Creating ssh")
    try:
      self.ssh_lock.acquire()
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      ssh.connect(dns_name, username="ubuntu", key_filename="turing-data-processing.pem")
    finally:
      self.ssh_lock.release()
      print("Ssh created")
    return ssh

  def send_files(self, ssh):
    print("Sending files")
    with SCPClient(ssh.get_transport()) as scp:
      scp.put('script.py', 'script.py')
      scp.put('url_list.csv', 'url_list.csv')
    print("Files sent")

  def receive_files(self, files, ssh):
    with SCPClient(ssh.get_transport()) as scp:
      for file in files:
        scp.get(file)

  def append_to_result(self, result):
    self.lock.acquire()
    try:
      self.result.extend(result)
    finally:
      self.lock.release()

  def get_result(self, stdout):
    result = []
    while True:
      line = stdout.readline()
      if not line:
        break
      try:
        result.append(json.loads(line.replace('\'','"')))
      except:
        print(line)
        print(traceback.print_exc())
    self.results_count.append(len(result))
    self.append_to_result(result)

  def start_instance_processsing(self, instance_num):
    try:
      instance = self.ec2.Instance(self.instance_ids[instance_num])
      ssh = self.create_ssh_client(instance.public_dns_name)
      self.send_files(ssh)
      stdin, stdout, stderr = ssh.exec_command(f'python3 script.py {instance_num+1} {self.size}')
      print("Command executed")
      self.get_result(stdout)
      self.receive_files([f'instance{instance_num+1}.log'], ssh)
      ssh.close()
    except Exception as e:
      print(f"Instance {instance.id} failed with error: {e}")
      print(traceback.print_exc())
    self.client.stop_instances(InstanceIds=[self.instance_ids[instance_num]])

if __name__=='__main__':
  num_instances = int(sys.argv[1])
  size = int(sys.argv[2])
  manager = ManageInstances(num_instances, size)
  threads = []
  for i in range(num_instances):
    thread = threading.Thread(target=manager.start_instance_processsing, args=(i,))
    thread.start()
    threads.append(thread)
  for thread in threads:
    thread.join()
  manager.create_file()
  print(manager.results_count)