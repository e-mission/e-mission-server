import logging
import attrdict as ad
import enum as enum


class WrapperBase(ad.AttrDict):
  """
  Base for our classes.
  Enhancements over AttrDict:
    - Specifies a list of valid properties. This is definitely NOT pythonic,
      but helps with self-documenting code. Otherwise, people want to use the
      class and they have no idea what the properties are. This also enables
      the following nice properties:
        - Read-only properties. We can mark the valid properties as read-only,
          which means that we can 
        - Support inline completion in ipython (my favorite feature!).
        - Support dependencies (properties that are automatically populated
          based on other properties for precomputation)
  """
  """
    All subclasses should define:
    - name: the name to use instead of AttrDict in the string representation
    - properties: a map of valid properties, with a value indicating whether
      they are read-only or not
  """
  Access = enum.Enum("PropertyAccess", "RO RW")


  def __init__(self, *args, **kwargs):
    super(WrapperBase, self).__init__(*args, **kwargs)

  def __dir__(self):
    return dir(super) + self.props.keys()

  def __getattr__(self, key):
    if key in self.props:
        return super(WrapperBase, self).__getattr__(key)
    else:
        raise AttributeError("property %s is not defined for %s" % (key, self.__class__.__name__))

  def __setattr__(self, key, value):
    if key in self.props:
        if self.props[key] == WrapperBase.Access.RW:
            return super(WrapperBase, self).__setattr__(key, value)
        else:
            raise AttributeError("property %s is read-only" % key)
    else:
        raise AttributeError("property %s is not defined for %s" % (key, self.__class__.__name__))


  def __repr__(self):
    """
    We would like to use a name for the class that is different from _AttrDict_.
    Unfortunately, the AttrDict name is hardcoded in the parent class, which
    makes sense since they don't want to do a lot of setup, but it means that we
    can't easily outgrow it.

    Out of the three options:
    1. Modify the parent class
    2. Duplicate the parent class
    3. Replace the string here, we have chosen to go with (3) for greatest cleanliness.
    """
    parentResult = super(WrapperBase, self).__repr__()
    return parentResult.replace("AttrDict", self.__class__.__name__)
