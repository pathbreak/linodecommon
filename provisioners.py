import os
import collections
import re
import subprocess
import time
import select

import simplejson as json


import logger


class BaseProvisioner(object):
    pass
    
    
    
class AnsibleProvisioner(BaseProvisioner):
    
    def provision(self, linode):
        pass
        
    def exec_playbook(self, targets, playbook_file, variables = None):
        
        if isinstance(targets, str):
            targets = targets + ','
            
        elif isinstance(targets, collections.Iterable):
            if len(targets) == 1:
                targets = targets[0] + ','
            else:
                targets = ','.join(targets)
            
        args = ['ansible-playbook', playbook_file, '-i', targets, '-u', 'root']
        
        if variables:
            json_vars = json.dumps(variables)
            args.extend(['-e', json_vars])
        
        env = os.environ.copy()
        env['ANSIBLE_STDOUT_CALLBACK'] = 'json'
        env['ANSIBLE_HOST_KEY_CHECKING'] = 'False'
        env['ANSIBLE_FORCE_COLOR']='true'
        
        # We want to see what Ansible's output as it happens, especially for long playbooks.
        # However, p.communicate() does not support that.
        # So we use select module to non-blockingly poll to see if it's stdout is ready to be read,
        # and read a chunk of data whenever it becomes available, without blocking.
        # But because this technique requires the process to *explicitly flush stdout periodically*
        # and python does not do that by default, instead buffering all output, this technique does
        # not work as it is. We have to set a special env var PYTHONUNBUFFERED=1 to force python 
        # to flush stdout.
        env['PYTHONUNBUFFERED'] = '1' 

        p = subprocess.Popen(args, stdin = None, stdout = subprocess.PIPE, close_fds=True, env = env)

        #stdoutdata, stderrdata = p.communicate()
        
        stdoutdata = ''
        dataend = False
        while (p.returncode is None) or (not dataend):
            p.poll() # This is required to set p.returncode
            dataend = False

            ready = select.select([p.stdout], [], [], 1.0)

            if p.stdout in ready[0]:
                data = p.stdout.read(256)
                if len(data) == 0: # Read of zero bytes means EOF
                    dataend = True
                else:
                    stdoutdata += data
                    print(data)
        
        
        json_output = self._extract_json_output(stdoutdata)
        ret = json.loads(json_output, object_pairs_hook = collections.OrderedDict)
        
        logger.msg(json_output)
        
        # TODO Instead of dumping entire JSON output, do some extraction
        # of useful info such as number of successful tasks, failed tasks, etc
        # and return that.
        
        return ret


        
    def _extract_json_output(self, output):
        '''
        Ansible mixes debug output and task output. 
        Luckily, it's not interspersed, and can be easily extracted.
        '''
        m = re.search('^{[\n ]+"plays', output, re.MULTILINE)
        json_start_pos = m.start()
        
        m = re.search('^}$', output, re.MULTILINE)
        json_end_pos = m.end()
        
        json_output = output[json_start_pos:json_end_pos]

        return json_output



    def wait_for_ping(self, linode, timeout, poll_interval):
        poll_count = timeout / poll_interval
        
        for i in range(poll_count):
            time.sleep(poll_interval) 
            
            if self.ping(linode):
                return True
        
        return False
        
        

    def ping(self, linode):
        
        target = linode.public_ip[0]
        target = target + ','
        args = ['ansible', 'all', '-i', target, '-u', 'root', '-m', 'ping']
        env = os.environ.copy()
        env['ANSIBLE_HOST_KEY_CHECKING'] = 'False'

        p = subprocess.Popen(args, stdin = None, stdout = subprocess.PIPE, close_fds=True, env = env)

        stdoutdata, stderrdata = p.communicate()
        print(stdoutdata)
        print(stderrdata)
        
        return p.returncode == 0
            
        
        
    
    
