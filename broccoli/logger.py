import time
import logging
from . interfaces import Logger
from . utils import get_colorizer, color


class ConsoleLogger(Logger):

    debug_color = color.silver
    info_color = color.white
    warning_color = color.olive
    error_color = color.maroon

    def __init__(self, name) -> None:
        self.name = name
        self.colorize = get_colorizer()
        self.format = self.get_format()
        self.level = logging.DEBUG

    def get_format(self):
        def format(level, text):
            return '%.3f - %s - %s' % (
                time.time(), level, text)
        return format

    def debug(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('DEBUG', msg),
                            self.debug_color))

    def info(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('INFO', msg),
                            self.info_color))

    def warning(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('WARNING', msg),
                            self.warning_color))

    def error(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('ERROR', msg),
                            self.error_color))

    def setLevel(self, level: int) -> None:
        def _unlogged(msg, *args):
            pass
        for lvl in ('debug', 'info', 'warning', 'error'):
            if level > logging._nameToLevel[lvl.upper()]:
                setattr(self, lvl, _unlogged)
            else:
                self.__dict__.pop(lvl, None)
        self.level = level
