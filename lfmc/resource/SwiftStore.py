from swiftclient import client, exceptions
from lfmc.resource.Storable import Storable


class SwiftStore(Storable):

  def __init__(self, url, username, password, project, container):
    """Short summary.

    Parameters
    ----------
    url : type
        Description of parameter `url`.
    username : type
        Description of parameter `username`.
    password : type
        Description of parameter `password`.
    project : type
        Description of parameter `project`.
    container : type
        Description of parameter `container`.

    Returns
    -------
    type
        Description of returned object.

    """
      self.url = url
      self.username = usernmae
      self.password = password
      self.project_name = project
      self.container_name = container
    # self.url = 'https://keystone.rc.nectar.org.au:5000/v3/'
    # self.username = 'anthony.rawlins@unimelb.edu.au'
    # self.password = 'MDI3NjkwMzcwMjZjYmQz'
    # self.project_name = 'LFMC'
    # self.container_name = 'MODIS'
    self.swift = client.Connection(authurl=self.url, user=self.username, key=self.password,
                                   tenant_name=self.project_name, auth_version='3')

  def swift_put_modis(self, file_name):
    container_name = 'MODIS'
    return self.put(self.container_name, file_name)

  def swift_check_modis(self, object_name):
    success = False
    try:
      resp_headers = self.swift.head_object('MODIS', object_name)
      print("%s exists." % object_name)
      success = True
    except exceptions.ClientException as e:
      if e.http_status == '404':
        print("The object: %s was not found." % object_name)
      else:
        print("An error occured checking the existence of object: %s" % object_name)
    return success

  def swift_get_modis(self, object_name):
    if self.swift_check_modis(object_name):
      resp_headers, obj_contents = self.swift.get_object('MODIS', object_name)
      with open(object_name, 'w+b') as so:
        so.write(obj_contents)
      return True
    else:
      return False

  def swift_put_lfmc(self, file_name):
    container_name = 'LFMC'
    return self.put(container_name, file_name)

  def put(self, container_name, file_name):
    with open(file_name, 'r+b') as local:
      self.swift.put_object(container_name, file_name,
                            contents=local, content_type='application/binary')
    return self.swift_check_modis(file_name)
