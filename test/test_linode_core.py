import linode_core
import simplejson as json

def test_create_linode_from_image():
    core = linode_core.Core({'conf-dir' : '../../test'})
    
    test_linode_spec = {
            'plan_id' : 1,
            'datacenter' : 9,
            'image' : 'testimage',
            'kernel' : 'Latest 64 bit',
            'label' : 'test',
            'group' : 'temporary',
            'disks' :   {
                            'boot' : {'disk_size' : 5000},
                            'swap' : {'disk_size' : 'auto'},
                                    
                            'others' : [
                                {
                                    'label' : 'mydata',
                                    'disk_size' : 5000,
                                    'type' : 'ext4'
                                }
                            ]
                        }

        }
        
    core.create_linode(test_linode_spec)


def test_linode_to_json():
    nodes = []
    for i in range(5):
        l = linode_core.Linode()
        l.public_ip = ['1.2.3.4']
        l.private_ip = '5.4.3.2'
        l.id = 123456
        
        nodes.append(l)
    
    print(json.dumps(nodes, default=lambda o:o.__dict__))
    
if __name__ == '__main__':
    #test_create_linode_from_image()
    test_linode_to_json()
