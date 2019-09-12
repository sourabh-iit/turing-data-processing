import csv
import os
import glob
import shutil
from time import process_time, clock, time
import threading
import sys
import re
import json
from modulefinder import ModuleFinder
import logging
import subprocess
from bs4 import BeautifulSoup
import requests

class ProcessInstance:
  def __init__(self, instance, size, num_threads):
    self.instance = instance
    self.size = size
    self.urls = []
    self.count = 0
    self.result = []
    self.num_threads = num_threads
    logging.basicConfig(filename=f'instance{instance}.log')
    self.lock = threading.Lock()
    self.get_urls()
    self.get_python_libraries()

  def get_urls(self):
    """get list of urls"""
    with open('url_list.csv','r') as f:
      data = csv.reader(f)
      for row in data:
        self.urls.extend(row)

  def get_python_libraries(self):
    try:
      with open('libraries.json','r') as f:
        self.python_libraries = json.load(f)
    except:
      res = requests.get('https://docs.python.org/3/py-modindex.html')
      elems = BeautifulSoup(res.content, 'lxml').find_all('code')
      libraries = []
      for el in elems:
        libraries.append(el.text)
      with open('libraries.json', 'w+') as f:
        json.dump(libraries, f)
      self.python_libraries = libraries

  def all_required_files(self, folderName):
    """
    Returns relative path of all python files and files containing dependencies in a folder
    """
    python_files = []
    directories = []
    for folderName, _ , filenames in os.walk(folderName):
      directories.append(folderName.split('/')[-1])
      for filename in filenames:
        extension = filename.split('.')[-1]
        if extension=='py':
          python_files.append(os.path.join(folderName,filename))
          directories.append(filename)
    return python_files, directories

  def delete_scope_variables(self, indentation, all_variables):
    """ Delete varialbes defined in given scope """
    variables = {}
    for word in all_variables:
      if all_variables[word]<=indentation:
        variables[word] = all_variables[word]
    return variables

  def first_word(self, line):
    """ Returns first word in all cases """
    return line.strip().split()[0].split('(')[0].split(':')[0].strip()
  
  def forloop_parameters(self, line):
    """ Returns list of varaibles defined in for loop (it should be for loop for sure) """
    words = []
    for word in line.strip().split('in')[0].split('for')[1].split(','):
      word = word.replace('(','')
      word = word.replace(')','')
      word = word.strip()
      words.append(word)
    return words

  def num_variables(self, line, all_variables, last_indentation, indentation):
    """
    Returns number of new variables defined in given line
    """
    new_variables = 0
    # indentation signifies scope here
    all_variables = self.delete_scope_variables(indentation, all_variables)    # delete variables defined in scope higher than current one
    line = line.strip()
    first_word = self.first_word(line)

    # get varialbes inside for loop
    if first_word=='for':
      for word in self.forloop_parameters(line):
        new_variables += 1
        if word not in all_variables:
          all_variables[word] = indentation+1
          new_variables += 1
      return new_variables, all_variables
    elif first_word not in ['if','def','elif','while','assert','print','return']:
      split_line = line.split("=")
      if len(split_line)>1:   # check if it contains "="
        if len(split_line[0].split('('))>1:
          return 0, all_variables
        words = split_line[0].strip().split(',')
        for word in words:
          word_split = word.strip().split('[')[0].split('.')    # check for dict and object
          if len(word_split)>1:
            if word_split[0]=='self':   # check for object attributes
              word = word_split[1]
              indentation = 1
            else:
              continue
          else:
            word = word_split[0]
          if word not in all_variables:
            all_variables[word] = indentation
            new_variables += 1
    return new_variables, all_variables

  def function_parameters(self, line):
    """
    Returns -1 if line does not contain function definition
    else returns number of parameters
    """
    line = line.strip()
    try:
      if len(line)>3 and line[:4]=="def ":
        parameters = line[3:].split('(')[1].split(')')[0].split(',')
        if parameters[0].strip()=="self" or parameters[-1]=='':
          return len(parameters)-1
        return len(parameters)
    except Exception as e:
      logging.warning(f'Error in line {line}: {e}')
    return -1

  def calc_tab_size(self, line):
    """ Calculates tab size of line """
    count=0
    n=len(line)
    while count<n and line[count]==' ':
      count+=1
    return count

  def count_indentation(self, line, tab_size):
    """ counts indentation level using tab size """
    n = len(line)
    index = 0
    count = 0
    while index<n and line[index]==' ':
      index+=tab_size
      count+=1
    return count

  def extra_opening_brackets(self, line):
    """ returns number number of opened brackets """
    extra_op_brackets = 0
    for c in line.strip():
      if c=='(' or c=='[' or c=='{':
        extra_op_brackets += 1
      elif c==')' or c==']' or c=='}':
        extra_op_brackets -= 1
    return extra_op_brackets

  def remove_comment_from_last(self, line):
    stack = []
    for i,c in enumerate(line):
      if c=='"':
        if len(stack)>0 and stack[-1]=='"':
          stack.pop()
        else:
          stack.append(c)
      elif c=="'":
        if len(stack)>0 and stack[-1]=="'":
          stack.pop()
        else:
          stack.append(c)
      elif c=='#':
        if len(stack)==0:
          return line[:i]
    return line

  def external_libraries(self, lines, directories):
    """ Returns all external libraries used """
    libraries = []
    for line in lines:
      line = line.replace("'","").replace('"','')
      try:
        modules = []
        split_line = line.strip().split('\n')[0].split()
        if len(split_line)==0:
          continue
        if split_line[0]=='from':
          module = line.split(';')[0].split('import')[0].split('from')[1]
          if '/' in module:
            continue
          if len(module.split('.'))>1:
            if module.split('.')[0].strip()!='':
              modules = [module.split('.')[0].strip()]
          else:
            modules = [module.strip()]
        elif split_line[0]=='import':
          line = line.split(';')[0]
          modules = [module.strip().split('.')[0] for module in line.split('import')[1].split(',')]
        else:
          continue
        for module in modules:
          if module=='':
            continue
          module = module.split(' as ')[0]
          # external module which is not python module and not local module
          if module not in self.python_libraries and module not in directories and module!='settings':
            libraries.append(module)
      except Exception as e:
        logging.exception(str(e))
    return libraries

  def get_data_for_file(self, filename, directories):
    """
    Returns total for loops and their nested depth, total functions defined and their total parameters,
    total variables deined, total lines and number of duplicates in a file
    """
    total_forloops = 0
    total_depth = 0
    inside_forloop = False
    current_forloop_indent = 0
    current_forloop_depth = 0
    forloop_start_indent = 0

    total_function_definitions = 0
    parameters_used = 0
    
    all_variables = {}
    total_variables = 0
    last_indentation = 0
    tab_size = 0
    if_else_depth = 0
    
    hash_map = {}
    lines = []
    match_hash = {}
    index = -1
    
    mlc_start = False # multi line comment
    
    slash_bracket = False
    opening_brackets = 0
    
    with open(filename, 'r', encoding="utf8", errors='ignore') as f:
      for line in f.readlines():
        try:
          stripped_line = line.strip()
          # check for multi line comments
          if len(stripped_line)>2:
            if stripped_line[0]=='"' and stripped_line[1]=='"' and stripped_line[2]=='"':
              mlc_start = not mlc_start
            if len(stripped_line)>3 and stripped_line[-1]=='"' and stripped_line[-2]=='"' and stripped_line[-3]=='"':
              mlc_start = False
              continue
            if mlc_start:
              continue
          # remove empty lines and commented lines
          if stripped_line=="" or (opening_brackets==0 and stripped_line[0]=='"') or stripped_line[0]=="\n" or stripped_line[0]=='#':
            continue
          
          line = self.remove_comment_from_last(line)

          line = line[0:-1]       # remove \n from last

          # check for statements spanning over multiple lines and bring them to one
          starting_value = opening_brackets   # we will need it later
          opening_brackets += self.extra_opening_brackets(line)
          if opening_brackets!=0:  # if not balanced
            if starting_value==0:
              lines.append(line)
            else:
              lines[-1]+=line
            continue
          elif starting_value!=0: # if it is end of bracket
            lines[-1]+=line
          elif line[-1]=='\\':    # check for line ending with \
            line = line[0:-1]
            if not slash_bracket: # if previous line does not have slash, it is a new line
              lines.append(line)
              slash_bracket=True
            else:
              lines[-1]+=line
            continue
          else:
            if slash_bracket:     # if previous line has slash then this line is last of previous line
              lines[-1]+=line
              slash_bracket=False
            else:
              lines.append(line)

          line = lines[-1]
          index += 1
          
          # hash_map contains list of indexes of line in lines list
          # for ex: if line "continue" is at indexes 23 and 76 in lines then hash_map["continue"]=[23,76]
          stripped_line = line.strip()
          if stripped_line not in hash_map:
            hash_map[stripped_line] = []
          hash_map[stripped_line].append(index)

          # calculate while tab size is not zero
          if tab_size == 0:
            tab_size = self.calc_tab_size(line)

          # Count indentation based on tab size. It refers scope
          line_indentation = self.count_indentation(line, tab_size)

          # Implementation of logic that conditionals should not have scope
          if if_else_depth>0:
            if line_indentation<last_indentation:
              if_else_depth -= last_indentation - line_indentation
              if if_else_depth<0:
                if_else_depth = 0
          indentation = line_indentation - if_else_depth    # conditional do not have scope
          if line.strip().split()[0].split('(')[0].split(':')[0] in ['if','else','elif']:
            if_else_depth += 1
          
          parameters = self.function_parameters(line)
          if parameters>=0:
            total_function_definitions += 1
            parameters_used += parameters
          if self.first_word(line) == 'for':
            if current_forloop_depth == 0:      # new root for loop
              current_forloop_depth = 1
              current_forloop_indent = indentation
              forloop_start_indent = indentation
              total_forloops += 1
            elif indentation>current_forloop_indent:    # child for loop
              current_forloop_indent = indentation
              current_forloop_depth += 1
          else:
            if current_forloop_depth>0:       # a root for loop that has not ended 
              if indentation<=forloop_start_indent:   # condition to be out from root for loop
                total_depth += current_forloop_depth
                current_forloop_depth = 0
                current_forloop_indent = 0
                forloop_start_indent = 0

          new_variables, all_variables = self.num_variables(line, all_variables, last_indentation, indentation)
          total_variables += new_variables
          last_indentation = line_indentation
        except Exception as e:
          logging.error(f"Error in line {line}: {e}")
    
    duplicates = 0
    # traverse hash_map and check for duplicates
    for line in hash_map:
      n = len(hash_map[line])
      # if a line occurs more than once in file
      if n>1:
        indexes = hash_map[line]
        # check every possible pair in list
        for i in range(n):
          for j in range(i+1,n):
            is_duplicate = True
            # check next three lines if exists
            for k in range(1,4):
              if indexes[i]+k>index or indexes[j]+k>index or lines[indexes[i]+k]!=lines[indexes[j]+k]:
                is_duplicate=False
                break
            if is_duplicate:
              for k in range(0,4):
                exists = indexes[i]+k in match_hash
                if not exists or (exists and match_hash[indexes[i]+k]!=indexes[j]+k):
                  duplicates += 1
                  match_hash[indexes[i]+k]=indexes[j]+k
    
    if index>-1:
      dup_percent = (duplicates*100)/(index+1)
    else:
      dup_percent = 0 # avoid zero division error
    
    return dup_percent, index+1, total_function_definitions, parameters_used, total_variables, total_forloops, total_depth, self.external_libraries(lines, directories)

  def add_data(self, data):
    self.lock.acquire()
    try:
      self.result.append(data)
    finally:
      self.lock.release()
  
  def print_data(self, data):
    self.lock.acquire()
    try:
      print(data)
      self.count+=1
    finally:
      self.lock.release()

  def process(self, num):
    """ Passes through all urls and select some from it. Downloads those repo, process them and then delete. """
    for i in range((self.instance-1)*self.size+1, (self.instance-1)*self.size+size+1):
      # evenly distribute load among threads
      if (i-1)%self.num_threads==num:
        # clone repository
        url = f"https://ksjdhf:kdjh@{self.urls[i].split('//')[1]}"
        # self.print_data(f'Getting response from {self.urls[i]}')
        res = subprocess.call(f"git clone {url}.git", shell=True)
        folderName = self.urls[i].split('/')[-1]
        # self.print_data(f'Got response from {self.urls[i]}')
        try:
          if res==0:
            python_files, directories = self.all_required_files(folderName)
            total_function_definitions = 0
            total_parameters_used = 0
            total_variables_used = 0
            total_lines = 0
            total_forloops = 0
            total_depth_of_forloops = 0
            external_libraries_used = []
            duplicates = []
            for file in python_files:
              try:
                duplication_data, lines, function_definitions, parameters_used, variables_used, forloops, forloops_depth, libraries = self.get_data_for_file(file, directories)
                duplicates.append(duplication_data)
                total_function_definitions += function_definitions
                total_parameters_used += parameters_used
                total_variables_used += variables_used
                total_lines += lines
                total_forloops += forloops
                total_depth_of_forloops += forloops_depth
                external_libraries_used.extend(libraries)
              except Exception as e:
                logging.exception(str(e))
            self.print_data({
              'repository_url': self.urls[i],
              'number of lines': total_lines,
              'libraries': list(set(external_libraries_used)),
              'nesting factor': total_depth_of_forloops/total_forloops if total_forloops>0 else 0,
              'average parameters': total_parameters_used/total_function_definitions if total_function_definitions!=0 else 0,
              'average variables': total_variables_used/total_lines if total_lines!=0 else 0,
              'code duplication': sum(duplicates)/len(duplicates) if len(duplicates)!=0 else 0 #average of all files
            })
          else:
            self.print_data("null")
        except Exception as e:
          # log errors
          logging.exception(f"Unable to process repo: {self.urls[i]}. Error: {str(e)}")
        finally:
          # finally delete repository if exists
          try:
            shutil.rmtree(folderName)
          except:
            pass
          pass

# main program
if __name__=='__main__':
  threads = []
  num_threads = 20
  instance = int(sys.argv[1])
  size = int(sys.argv[2])
  try:
    subprocess.call(f'rm instance{instance}.log', shell=True)
  except:
    pass
  manager = ProcessInstance(instance, size, num_threads)
  # create threads
  for _ in range(num_threads):
    thread = threading.Thread(target=manager.process, args=(_,))
    thread.start()
    threads.append(thread)
  # wait for threads to complete
  for thread in threads:
    thread.join()
  # print(manager.result)