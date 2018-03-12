class Storable:
  def object_exists(self, obj):
    return True

  def store_object(self, obj):
    return True

  def list_objects(self, path):
    return []

  # TODO - Object Deletion
