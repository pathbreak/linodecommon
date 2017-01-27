from image_manager import Image, ImageManager, LinodeImageProvider

def test_create_image():
    o = LinodeImageProvider({'conf-dir' : '../../test'})
    
    img = Image('testimage', 'linode', {
        'datacenter' : 'singapore',
        'distribution' : 'Ubuntu 14.04 LTS',
        'kernel' : 'Latest 64 bit',
        'type' : 'linode-image',
        'cluster-type' : 'gluster'
    })
    
    def test_provisioner_func(linode):
        return True
                
    o.create_image(img, test_provisioner_func)



def test_create_disk_from_image():
    o = ImageManager({'conf-dir' : '../../test'})
    o.create_disk_from_image('testimage', None)



if __name__ == '__main__':
    #test_create_image()
    test_create_disk_from_image()
    
