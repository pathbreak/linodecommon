import time
import Queue
import threading
import collections
import traceback

import linode_api as lin
import image_manager

from passwordgen import pattern

import simplejson as json
import yaml

import logger

from exc import CreationError

class Core(object):
    
    def __init__(self, app_ctx):
        assert app_ctx
        self.app_ctx = app_ctx
        
        
        
    def create_linode(self, linode_spec, boot = True, delete_on_error = True):
        ''' Create a linode.
        
        Args:
            linode_spec : A `dict` with all details required to create a linode. For example:
                {
                    'plan_id' : 1,
                    'datacenter' : 9,
                    'distribution' : 'CentOS 7', 
                        or 
                    'image' : 'image_label', 
                    'kernel' : 'Latest 64 bit',
                    'label' : 'myserver',
                    'group' : 'mycluster',
                    
                    'disks' :   {
                                    'boot' : {
                                        'disk_size' : 5000
                                    },
                                    
                                    'swap' : {
                                        'disk_size' : 'auto'
                                    },
                                    
                                    'others' : [
                                        {
                                            'label' : 'mydata',
                                            'disk_size' : 18000,
                                            'type' : 'ext4'
                                        }
                                    ]
                                }
                }
        
                linode_spec['disks']['others'] is optional.
                linode_spec['disks']['others'][...]['type'] should be [ext4 | ext3 | swap | raw]
        
        Returns:
            A Linode object.
        '''
        
        assert any( [linode_spec.get('distribution'), linode_spec.get('image')] )
        
        logger.msg("Create node")
        
        linode = Linode()
        linode.inited = False
        
        linode_id = None
        
        try:
            success, linode_id, errors = lin.create_node(linode_spec['plan_id'], linode_spec['datacenter'])
            linode.created = success
            linode.id = linode_id
            if not success:
                logger.error_msg("Create node failed." + errors)
                raise CreationError()
                
            logger.msg("Created node %d" % (linode.id))
            
            logger.msg("Update node label")
            label = linode_spec['label']
            if '{linode_id}' in label:
                label = label.replace('{linode_id}', str(linode_id))
            success, linode_id, errors = lin.update_node(linode_id, label, linode_spec['group'])
            if not success:
                logger.warning_msg("Update node failed but continuing." + errors)
                
            # If update node fails, don't abort because it's not a critical failure.
            
            # Linode requires passwords to have atleast 2 of these 4 classes - lowercase, uppercase, numbers, digits.
            # See https://github.com/nkrim/passwordgen for understanding the pattern.
            # TODO Use Vault here
            root_password = pattern.Pattern('%{cwds+^}[64]').generate()
            root_ssh_key_file = '/home/karthik/.ssh/id_rsa.pub'
            
            jobs = []
            
            disks = []
            
            distro = linode_spec.get('distribution')
            image_label = linode_spec.get('image')
            
            if image_label:
                logger.msg("Create boot disk from image '%s'" % (image_label)) 
                
                img_mgr = image_manager.ImageManager(self.app_ctx)
                
                disk_spec = {
                    'linode_id' : linode_id,
                    'label' : 'boot', 
                    'disk_size' : linode_spec['disks']['boot']['disk_size'], 
                    'root_password' : root_password,
                    'root_ssh_key_file' : root_ssh_key_file
                }
                
                success, disk_details, errors = img_mgr.create_disk_from_image(image_label, disk_spec)
                
                if not success:
                    logger.error_msg("Create disk from image failed." + errors)
                    raise CreationError()
                
                assert disk_details['disk_id']
                disk_id = disk_details['disk_id']
                
            elif distro:
                logger.msg("Create boot disk from distribution")
                
                success, disk_id, disk_job_id, errors = lin.create_disk_from_distribution(linode_id, 
                    linode_spec['distribution'], linode_spec['disks']['boot']['disk_size'], 
                    root_password, root_ssh_key_file)
                
                if not success:
                    logger.error_msg("Create disk from distribution failed." + errors)
                    raise CreationError()
            
            
                jobs.append( (linode_id, disk_job_id) )
                print("Creating boot disk %d, job %d" % (disk_id, disk_job_id))
                
            linode.boot_disk_id = disk_id
            disks.append(disk_id)
            
            
            swap_disk = linode_spec['disks'].get('swap')
            if swap_disk is not None:
                logger.msg("Create swap disk")
                swap_disk_size_mb = swap_disk['disk_size']
                if str(swap_disk_size_mb) == 'auto':
                     swap_disk_size_mb = None
                    
                success, swap_disk_id, swap_job_id, errors = lin.create_swap_disk(linode_id, swap_disk_size_mb)
                if not success:
                    logger.error_msg("Create swap disk failed." + errors)
                    raise CreationError()
                    
                jobs.append( (linode_id, swap_job_id) )
                disks.append(swap_disk_id)
            
            
            other_disks = linode_spec['disks'].get('others')
            if other_disks is not None:
                logger.msg('Create additional disks')
                
                for other_disk in other_disks:
                    
                    # disk type should be one of 'ext4|ext3|swap|raw'. If
                    # it's a different filesystem, leave it as raw and create
                    # filesystem during provisioning.
                    disk_type = other_disk['type']
                    if disk_type not in ['ext4', 'ext3', 'swap']:
                        disk_type = 'raw'
                    
                    success, other_disk_id, other_disk_job_id, errors = lin.create_disk(
                        linode_id, 
                        disk_type, 
                        other_disk['disk_size'], 
                        other_disk['label'])
                        
                    if not success:
                        logger.error_msg('other disk creation failed.' + errors)
                        raise CreationError()
                    
                    logger.msg('Created additional disk:%d' % (other_disk_id))
                    
                    jobs.append( (linode_id, other_disk_job_id) )
                    disks.append(other_disk_id)
            
                
            results = self.wait_for_jobs(jobs)
            
            for r in results:
                if not r['success']:
                    logger.error_msg("Job failed. Aborting")
                    print(r)
                    raise CreationError()
            
            print("Create configuration")
            success, config_id, errors = lin.create_config(linode_id, linode_spec['kernel'], 
                disks,  'testconfig')
            if not success:
                logger.error_msg('Configuration failed.' + errors)
                raise CreationError()
            
            print("Configure private IP")
            success, linode.private_ip = lin.add_private_ip(linode_id)
            if not success:
                print("Private IP failed")
                raise CreationError()
            print('Private IP: %s' % (linode.private_ip))
            
            linode.public_ip = [lin.get_public_ip_address(linode_id)]
            print('Public IP: %s' % (linode.public_ip))
            
            logger.success_msg('Linode Created')
            
            if boot:
                print("Booting")
                success, boot_job_id, errors = lin.boot_node(linode_id, config_id)
                if not success:
                    logger.error_msg('Booting failed.' + errors)
                    raise CreationError()

                finished, success = self.wait_for_job(linode_id, boot_job_id)
                if not success:
                    logger.error_msg('Booting failed')
                    raise CreationError()
                    
                logger.success_msg('Linode Booted')
                
            linode.inited = True
            
            return linode
            
        except Exception as e:
            
            if delete_on_error:
                # Delete the temporarily created linode.
                logger.error_msg('Deleting node due to error:%s\n%s' % (e, traceback.format_exc()))
                deleted, _, errors = lin.delete_node(linode_id, True)
                if not deleted:
                    logger.warn_msg('Warning: Unable to delete node. Please delete from Linode Manager.' + errors)
                
            return None


    def wait_for_jobs(self, linodes_jobs):
        # Multithreaded wait for jobs
        # linodes_jobs is a list of (linode_id, job_id) tuples
        #
        # Returns an iterable with (linode_id, job_id, finished, success) tuples
        
        def job_waiter(q, results):
            linode_id, job_id = q.get()
            finished, success = self.wait_for_job(linode_id, job_id)
            results.append( {
                'linode_id' : linode_id, 
                'job_id' : job_id, 
                'finished' : finished, 
                'success' : success} )
            q.task_done()
            
            
        q = Queue.Queue()
        results = collections.deque()
        
        threads = []
        for linode_id, job_id in linodes_jobs:
            t = threading.Thread( target = job_waiter, args = (q, results) )
            t.start()
            threads.append(t)

        for item in linodes_jobs:
            q.put(item)

        # block until all tasks are done
        q.join()

        # stop workers
        for t in threads:
            t.join()
            
        return results
            
        
    def wait_for_job(self, linode_id, job_id):
        timeout = 240 # 4 minutes
        poll_interval = 5 # seconds
        poll_count = timeout / poll_interval
        
        for i in range(poll_count):
            time.sleep(poll_interval)
            finished, success = lin.is_job_finished(linode_id, job_id)
            if finished is None:
                logger.error_msg('No such job %d for linode %d' % (job_id, linode_id))
                break
            
            if finished is True:
                logger.msg('Finished job %d for linode %d' % (job_id, linode_id))
                break
       
        return finished, success


class Linode(object):
    def __init__(self):
        pass
