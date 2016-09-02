import inspect
import sys

def arity(fun):
  if sys.version_info.major == 2:
    return len(inspect.getargspec(fun).args)
  else:
    return len(inspect.signature(fun).parameters)
