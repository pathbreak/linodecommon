import os
import os.path

import collections

import time
import traceback

import linode_core
import linode_api as lin

from exc import CreationError

import logger

import simplejson as json



class ImageManager(object):
    '''
    Responsible for managing disk images, which are a quick way to create new machines
    by cloning, with all required software already installed and configured prior to creation.

    - maintains an inventory of "images"

    - responsible for both "imagizing a disk" and "creating a disk from an image"

    - an image may be implemented as an actual linode image or is just a file stored on an image host

    - responsible for creating a disk based on any image, regardless of underlying implementation of that image

    - it's possible that all the cluster scripts want to share a common image host. So this class should be designed
      as a common component that can be used by any of the cluster scripts, but show only cluster specific data - ie,
      the gluster script need not know anything about ceph images
    
    '''
    
    def __init__(self, app_ctx):
        '''
        Create an image manager object.
        
        Args:
            - app_ctx : Application definied settings such as the configuration directory to use.
        '''
        assert type(app_ctx) is dict and app_ctx.get('conf-dir')
        self.app_ctx = app_ctx
        
        
        
        
    def create_image(self, image, provisioner, delete_on_error = True):
        '''
        Args:
            - image : an :class:`Image` object
        '''
        assert image is not None
        
        if image.provider == 'linode':
            linode_provider = LinodeImageProvider(self.app_ctx)
            linode_provider.create_image(image, provisioner, delete_on_error)
            
        else:
            raise ValueError("Unsupported image provider: %s" % (image.provider))
        
        
    def create_disk_from_image(self, image_label, disk_spec):
        assert image_label
        
        image = self.load_image(image_label)
        if image is None:
            return (False, None, ['No such image %s' % (image_label)])
        
        if image.provider == 'linode':
            linode_provider = LinodeImageProvider(self.app_ctx)
            success, disk_details, errors = linode_provider.create_disk_from_image(image, disk_spec)
            return (success, disk_details, errors)
            
        else:
            raise ValueError("Unsupported image provider: %s" % (image.provider))
        
        
        
    def load_image(self, image_label):
        
        if not self.check_image_exists(image_label):
            logger.error_msg("Image '%s' does not exist." % (image_label))
            return None
            
        image_conf_dir = os.path.join(self.app_ctx.get('conf-dir'), 'images')
        image_filename = os.path.join(image_conf_dir, image_label, 'image.json')
        try:
            with open(image_filename, 'r') as f:
                image_details = json.load(f, object_pairs_hook = collections.OrderedDict)
            
            logger.msg("Image details read from '%s'" % (image_filename))
        except IOError as e:
            logger.error_msg("Cannot read image file '%s'" % (image_filename))
            return None
            
        image = Image(image_label, image_details['provider'], image_details)
        return image


    def check_image_exists(self, image_label):
        image_conf_dir = os.path.join(self.app_ctx.get('conf-dir'), 'images')
        image_dir = os.path.join(image_conf_dir, image_label)
        return os.path.exists(image_dir)
        
   


class Image(object):
    
    def  __init__(self, label, provider, image_spec):
        '''
        An Image represents a disk image.
        
        Args:
            - label : a unique label for this image.
            - provider : str. Provider of an image.
            - image_spec : dict. Provider specific information for creating and managing the image.
        '''
        self.label = label
        self.provider = provider
        self.spec = image_spec
        
        
        
        
class LinodeImageProvider(object):
    
    def __init__(self, app_ctx):
        self.app_ctx = app_ctx
        self.image_conf_dir = os.path.join(self.app_ctx.get('conf-dir'), 'images')
        
        
    def create_image(self, image, provisioner, delete_on_error = True):
        
        if self.check_image_exists(image.label):
            logger.error_msg("Image with name '%s' already exists. Please use a different name." % (image.label))
            return False
        
        image_type = image.spec.get('type', None)
        assert image_type is not None
        assert image_type in ['linode-image', 'hosted-image']
        
        if image_type == 'linode-image':
            result = self.create_linode_image(image, provisioner, delete_on_error)
                
        elif image_type == 'hosted-image':
            result = self.create_hosted_image(image, provisioner)
        
        return result
            
                
    def create_linode_image(self, image, provisioner, delete_on_error = True):
        
        image_dir = os.path.join(self.image_conf_dir, image.label)
        try:
            os.makedirs(image_dir)
        except:
            logger.error_msg("Unable to create image directory:" + image_dir)
            return False
            
        
        image_spec = image.spec
        
        # Create and boot a temporary Linode.
        temp_linode_spec = {
            'plan_id' : 1,
            'datacenter' : image_spec['datacenter'],
            'distribution' : image_spec['distribution'],
            'kernel' : image_spec['kernel'],
            'label' : image.label,
            'group' : 'temporary',
            'disks' :   {
                            'boot' : {'disk_size' : 5000}
                        }

        }
        
        core = linode_core.Core(self.app_ctx)
        
        temp_linode = None

        image_id = None
        
        ret = False
        
        try:
            # If creation or booting fails, core will have already deleted the node.
            logger.msg('Creating a temporary linode for imaging')
            temp_linode = core.create_linode(temp_linode_spec, boot = True, delete_on_error = delete_on_error)
            if temp_linode is None:
                raise CreationError()
                
            # Run provisioning step
            if provisioner:
                # Don't try to SSH immediately after booting. 
                # Wait for a while for SSH daemon to come up, ping the machine, then start provisioning
                logger.msg('Waiting for node to initialize')
                pinged = provisioner.wait_for_ping(temp_linode, 60, 10)
                if not pinged:
                    logger.error_msg('Unable to reach node. Deleting' )
                    raise CreationError()
            
                logger.msg('Provisioning')
                result = provisioner.provision(temp_linode)
                if not result:
                    logger.error_msg('Image provisioning failed. Deleting' )
                    raise CreationError()
            
            # Shutdown the linode
            logger.msg('Shutting down')
            shutdown, job_id, errors = lin.shutdown_node(temp_linode.id)
            if not shutdown:
                logger.error_msg('Shutdown failed. Deleting.' + errors)
                raise CreationError()
                
            finished, success = core.wait_for_job(temp_linode.id, job_id)
            if not success:
                logger.error_msg('Shutdown failed. Deleting')
                raise CreationError()
            
            # Imagize the disk
            logger.msg('Imaging')
            success, image_id, job_id, errors = lin.create_diskimage(temp_linode.id, 
                temp_linode.boot_disk_id, 
                '%s' % (image.label))
            
            if not success:
                logger.error_msg('Imaging failed. ' + errors)
                raise CreationError()
                
            finished, success = core.wait_for_job(temp_linode.id, job_id)
            if not success:
                logger.error_msg('Imaging failed')
                raise CreationError()
                
            # Save image details
            self.save_image(image, image_id)
            
            logger.success_msg('Image created')
            ret = True
            
        except Exception as e:
            ret = False
            
            if delete_on_error:
                logger.error_msg('Deleting image due to error:%s\n%s' % (e, traceback.format_exc()))
                
                # If there's an error delete the image because we won't get the image ID again.
                if image_id is not None:
                    deleted, _, errors = lin.delete_image(image_id)
                    if not deleted:
                        logger.warn_msg('Warning: Unable to delete image. Please delete from Linode Manager. ' + errors)
                    
                # Delete the image folder too.
                os.rmdir(image_dir)
        
            
        finally:

            # Delete the temporarily created linode.
            if delete_on_error:
                if temp_linode:
                    logger.msg('Deleting temporary node created for imaging')
                    deleted, _, errors = lin.delete_node(temp_linode.id, True)
                    if not deleted:
                        logger.warn_msg('Warning: Unable to delete node. Please delete from Linode Manager.' + errors)
                
        return ret
            
        
    def create_hosted_image(self, image):
        pass
      
      
        
    def save_image(self, image, image_id):
        
        # Save image details
        image_spec = image.spec
        
        image_details = {
            'provider' : 'linode',
            'type' : image_spec['type'],
            'cluster-type' : image_spec['cluster-type'],
            'id' : image_id,
            'datacenter' : image_spec['datacenter'],
            'distribution' : image_spec['distribution'],
            'kernel' : image_spec['kernel']
        }
        
        image_filename = os.path.join(self.image_conf_dir, image.label, 'image.json')
        try:
            with open(image_filename, 'w') as f:
                json.dump(image_details, f, indent = 4 * ' ')
            
        except IOError as e:
            logger.error_msg('Cannot write image file. Deleting image')
            if os.path.exists(image_filename):
                os.remove(image_filename)
                
            raise CreationError()
            
            
            
    def create_disk_from_image(self, image, disk_spec):
        
        image_type = image.spec.get('type', None)
        assert image_type is not None
        assert image_type in ['linode-image', 'hosted-image']
        
        if image_type == 'linode-image':
            result = self.create_disk_from_linode_image(image, disk_spec)
                
        elif image_type == 'hosted-image':
            result = self.create_disk_from_hosted_image(image, disk_spec)
            
        return result
        
        

    def create_disk_from_linode_image(self, image, disk_spec):
        
        linode_image_id = image.spec['id']
        linode_id = disk_spec['linode_id']
        
        # TODO Password and SSH key should be handled better. Read from
        # vault. SSH key should not be a file.
        success, disk_id, job_id, errors = lin.create_disk_from_image(
            linode_id, 
            linode_image_id, 
            disk_spec['label'], 
            disk_spec['disk_size'], 
            disk_spec['root_password'], 
            disk_spec['root_ssh_key_file'])
            
        if not success:
            logger.error_msg('Create disk from linode image failed.' + errors)
            return (False, None, errors)
            
        core = linode_core.Core(self.app_ctx)
        finished, success = core.wait_for_job(linode_id, job_id)
        if not success:
            logger.error_msg('Create disk from linode image failed.')
            return None
            
        return (True, {'disk_id' : disk_id}, None)
        
    
    def create_disk_from_hosted_image(self, image, disk_spec):
        pass
        
        
    def check_image_exists(self, image_label):
        image_conf_dir = os.path.join(self.app_ctx.get('conf-dir'), 'images')
        image_dir = os.path.join(image_conf_dir, image_label)
        return os.path.exists(image_dir)



