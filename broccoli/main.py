import os
import argparse
import logging
from . logger import ConsoleLogger
from . utils import load_instance, load_class, fullclassname
from . utils import get_colorizer, color
from . interfaces import App, Worker, Plugin, Configurable


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-a',
                        '--app',
                        dest='app',
                        help='Application instance',
                        required=True)

    parser.add_argument('-w',
                        '--worker',
                        dest='worker',
                        help='Worker class',
                        default='prefork')

    parser.add_argument('-p',
                        '--plugins',
                        dest='plugins',
                        type=lambda x: [y.strip() for y in x.split(',')],
                        help='List of plugins',
                        default=argparse.SUPPRESS,
                        nargs='?')

    parser.add_argument('-l',
                        '--loglevel',
                        dest='loglevel',
                        help='Logging level',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO')

    parser.add_argument('--nocolors',
                        dest='nocolors',
                        help='No colors',
                        action='store_true',
                        default=False)

    args, remaining = parser.parse_known_args()

    if args.nocolors:
        os.environ['NOCOLORS'] = '1'

    colorize = get_colorizer()

    try:
        app = load_instance(args.app, instance_of=App)
    except (ImportError, TypeError):
        print(colorize('Could not load app %r' % args.app, color.red))
        raise

    worker_class = app.conf.get('worker') or args.worker

    try:
        worker_class = load_class(worker_class,
                                  'broccoli.worker',
                                  subclass_of=Worker)
    except (ImportError, TypeError):
        print(colorize('Could not load worker %r' % worker_class, color.red))
        raise

    worker_class.add_console_args(parser)

    if 'plugins' in args:
        plugins = args.plugins
    else:
        plugins = app.conf.get('plugins', ['MasterLogger'])

    plugin_classes = []
    for plugin in plugins:
        try:
            plugin_class = load_class(plugin,
                                      'broccoli.plugins',
                                      subclass_of=Plugin)
        except (ImportError, TypeError):
            print(colorize('Could not load plugin %r' % plugin, color.red))
            raise

        plugin_class.add_console_args(parser)
        plugin_classes.append(plugin_class)

    parser.add_argument('-h', '--help',
                        action='help',
                        default=argparse.SUPPRESS,
                        help='Show this help message and exit')

    args = parser.parse_args()
    conf = dict(app.conf)
    conf.update(args._get_kwargs())

    loglevel = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }[conf['loglevel']]

    logger = ConsoleLogger('broccoli')
    logger.setLevel(loglevel)

    conf['app'] = app
    conf['logger'] = logger
    plugins = [p(**conf) for p in plugin_classes]
    conf['plugins'] = plugins

    worker = worker_class(**conf)

    print_startup_info(
        ('App', app),
        ('Worker', worker),
        *((p.__class__.__name__ + ' plugin', p) for p in plugins)
        )

    logger.info('start server')
    worker.run()
    logger.info('stop server')


def print_startup_info(*args):
    index = 0
    indent = 0
    max_height = 16
    colorize = get_colorizer()

    def new_block():
        nonlocal index, indent
        index = 0
        if not lines:
            indent = 0
        else:
            indent = max(l[0] for l in lines) + 1

    def print_(text, endline=True, header=False, color=color.aqua):
        nonlocal index
        if index >= max_height:
            new_block()
        elif header and (index + 1) >= max_height:
            new_block()
        while index >= len(lines):
            lines.append([0, ''])
        line = lines[index]
        if line[0] < indent:
            line[1] += ' ' * (indent - line[0])
            line[0] = indent
        line[0] += len(text) + endline
        line[1] += colorize(text, color) + (' ' if endline else '')
        if endline:
            index += 1

    def print_kv(key, value, max_width=35):
        key = '. ' + key + ': '
        print_(key, color=color.tea, endline=False)
        if isinstance(value, int):
            clr = color.olive
        elif isinstance(value, str):
            clr = color.olive
        elif isinstance(value, (Configurable, type)):
            clr = color.green
            value = fullclassname(value)
        elif isinstance(value, (list, tuple)):
            clr = 3
            value = ', '.join(value)
        value = str(value)
        rest = max_width - len(key)
        indent = ''
        while value:
            print_(indent + value[:rest], color=clr)
            value = value[rest:]
            rest = max_width - 4
            indent = '    '

    def print_conf(conf):
        if not conf:
            print_('. enabled', color=color.tea)
        for key, value in conf.items():
            print_kv(key, value)
            if isinstance(value, Configurable):
                print_conf(value.get_applied_conf())

    lines = [
        [0, 0, 0, 0, 0, 0, 0, 233, 233, 58, 58, 149, 107, 149, 149, 106, 106,
            149, 149, 149, 107, 107, 64, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 234, 58, 106, 106, 106, 106, 106, 106, 106, 106, 106, 106,
            106, 106, 106, 106, 106, 106, 106, 106, 149, 64, 235, 0, 0, 0, 0,
            0, 0, 0],
        [0, 0, 235, 70, 106, 106, 106, 106, 106, 106, 106, 70, 106, 64, 106,
            64, 106, 70, 106, 64, 106, 106, 106, 106, 106, 106, 106, 106, 149,
            233, 0, 0, 0, 0],
        [0, 234, 70, 64, 106, 64, 64, 64, 64, 64, 64, 64, 70, 64, 64, 64, 64,
            64, 64, 64, 64, 106, 106, 106, 106, 70, 106, 106, 106, 235, 0, 0,
            0, 0],
        [0, 235, 70, 64, 64, 58, 22, 236, 235, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 70, 70, 64, 70, 70, 70, 106, 106, 106, 232, 0,
            0],
        [0, 237, 64, 64, 64, 22, 237, 58, 234, 236, 106, 236, 22, 58, 236, 58,
            106, 236, 64, 64, 64, 64, 64, 64, 64, 64, 64, 70, 64, 106, 106, 58,
            0, 0],
        [0, 235, 64, 64, 64, 22, 22, 235, 149, 237, 106, 100, 238, 58, 149,
            100, 58, 22, 236, 236, 64, 64, 64, 64, 64, 64, 64, 70, 64, 64, 106,
            106, 234, 0],
        [0, 0, 0, 0, 236, 22, 22, 22, 3, 149, 235, 149, 149, 238, 237, 149,
            236, 149, 149, 236, 236, 3, 22, 237, 236, 236, 235, 235, 235, 22,
            106, 106, 64, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 237, 143, 149, 149, 58, 106, 149, 143, 106,
            58, 149, 149, 3, 58, 3, 106, 100, 235, 22, 22, 22, 22, 2, 70, 238,
            0],
        [0, 0, 0, 0, 0, 0, 0, 235, 149, 149, 149, 149, 106, 149, 149, 149, 149,
            106, 149, 149, 149, 149, 100, 100, 58, 235, 22, 22, 22, 22, 64,
            234, 0, 0],
        [0, 0, 0, 0, 0, 0, 235, 149, 149, 149, 149, 100, 149, 149, 149, 149,
            106, 149, 149, 58, 238, 22, 22, 22, 22, 22, 22, 22, 22, 22, 58,
            235, 0, 0],
        [0, 0, 0, 0, 233, 3, 149, 149, 149, 149, 149, 149, 149, 149, 149, 149,
            149, 237, 0, 0, 0, 0, 0, 0, 237, 234, 234, 234, 234, 234, 0, 0, 0,
            0],
        [0, 0, 0, 235, 58, 235, 149, 149, 149, 149, 149, 149, 149, 149, 149,
            236, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 237, 228, 228, 143, 143, 58, 58, 149, 149, 149, 100, 234, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 58, 228, 228, 228, 234, 236, 149, 235, 0, 0, 0,
            'b', 'r', 'o', 'c', 'c', 'o', 'l', 'i', ' ', 'v', '0', '.', '1',
            0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 232, 237, 235, 58, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ]

    if colorize.support_colors:
        lines = [[len(line), (''.join([colorize(
                    ' ' if c == 0 else (c if type(c) is str else '#'),
                    c if type(c) is int else color.grey
                 ) for c in line]))] for line in lines]
    else:
        lines = []

    print()
    new_block()

    for title, obj in args:
        if not obj:
            continue
        print_('[%s]' % title.capitalize(), header=True, color=color.white)
        print_conf(obj.get_applied_conf())
        print_('')

    print('\n'.join(l[1] for l in lines))
    print()
