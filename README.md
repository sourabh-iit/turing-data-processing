# turing-data-processing
Extract data from 100000 github repositories using multiple aws instances

Requirements:
python 3.6

Dependencies to run script.py:

sudo apt-get install python3-bs4
sudo apt-get install python3-lxml

Dependencies to run ec2.py:

sudo apt-get install python3-pip
pip3 install boto3
pip3 install awscli
pip3 install paramiko
pip3 install scp

Config aws:
Run 'aws configure' - add aws_access_key_id, aws_secret_access_key and region 'us-east-2'

Create multiple ec2 instances and install script.py dependecies on them.

script.py - It takes two arguments instance_num and size. From list of urls, it takes number of urls equal to value of 'size' and starts from value instance_num*(size-1)+1. 
ec2.py - Starts ec2 instances and runs script 'script.py' on them. ec2.py takes two arguments num_instances and size. num_instances is number of instances of ec2 that you want to run and size is number of repos that you want script.py to run on.

How to run:
ec2.py - python3 ec2.py <num_instances> <size>
script.py - python3 script.py <instance_num> <size>
