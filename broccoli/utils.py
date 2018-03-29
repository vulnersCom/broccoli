import os
import sys
import typing
import importlib


def load_string(path: str, module: typing.Any=None) -> typing.Any:
    if '.' in path:
        module, attr = path.rsplit('.', 1)
        try:
            module = importlib.import_module(module)
        except Exception as e:
            raise ImportError(path) from e
        path = attr

    if isinstance(module, str):
        try:
            module = importlib.import_module(module)
        except Exception as e:
            raise ImportError(path) from e

    if module is None:
        raise ImportError(path)

    try:
        return getattr(module, path)
    except AttributeError:
        raise ImportError(path)


def load_class(path: typing.Any,
               module: typing.Any=None,
               subclass_of: typing.Type=None) -> typing.Type:
    if isinstance(path, str):
        ret = load_string(path, module)
    else:
        ret = path

    if not isinstance(ret, type):
        raise TypeError('%r is not a class.' % ret)

    if subclass_of and not issubclass(ret, subclass_of):
        raise TypeError('%r is not subclass of %r.' % (ret, subclass_of))

    return ret


def load_instance(path: typing.Any,
                  module: typing.Any=None,
                  instance_of: typing.Type=None) -> typing.Any:

    if isinstance(path, str):
        ret = load_string(path, module)
    else:
        ret = path

    if instance_of and not isinstance(ret, instance_of):
        raise TypeError('%r is not instance of %r.' % (ret, instance_of))

    return ret


def fullclassname(cls):
    if not isinstance(cls, type):
        cls = type(cls)
    if cls.__module__.startswith('broccoli'):
        return cls.__name__
    return '%s.%s' % (cls.__module__, cls.__name__)


class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.
    Optional ``name`` argument allows you to make cached properties of other
    methods. (e.g.  url = cached_property(get_absolute_url, name='url') )
    """
    def __init__(self, func, name=None):
        self.func = func
        self.__doc__ = getattr(func, '__doc__')
        self.name = name or func.__name__

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


class color:
    black = 0
    maroon = 1
    green = 2
    olive = 3
    navy = 4
    purple = 5
    tea = 6
    silver = 7
    grey = 8
    red = 9
    lime = 10
    yellow = 11
    blue = 12
    fuchsi = 13
    aqua = 14
    white = 15


def get_colorizer():
    if not sys.stdout.isatty() or os.environ.get('NOCOLORS'):
        return _fake_colorizer
    return _simple_colorizer


def _fake_colorizer(text, color):
    return text


_fake_colorizer.support_colors = False  # type: ignore


def _simple_colorizer(text, color):
    return '\x1b[38;5;%dm%s\x1b[0m' % (color, text)


_simple_colorizer.support_colors = True  # type: ignore


def reiter(v, seq):
    yield v
    yield from seq


def rewind(vals, chains):
    if not chains or not vals:
        return False, ()
    val, *rest_vals = vals
    chain, *rest_chains = chains
    chain = iter(chain)
    for v in chain:
        if v > val:
            return False, (reiter(v, chain), *rest_chains)
        elif v == val:
            restarted, rest_chains = rewind(rest_vals, rest_chains)
            return False, (
                chain if restarted else reiter(v, chain),
                *rest_chains
                )
    else:
        return True, chains
