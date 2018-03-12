from lfmc.resource.Storable import Storable

class LocalStorage(Storable):
  def __init__(self, path):
    self.path = path
