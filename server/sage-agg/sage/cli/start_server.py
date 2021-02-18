# start_server.py
# Author: Thomas MINIER - MIT License 2017-2019
import importlib.util
from os.path import isfile

import click
from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems
from sage.http_server.server import sage_app


class StandaloneApplication(BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


@click.command()
@click.argument("config")
@click.option("-p", "--port", type=int, default=8000, show_default=True, help="The port to bind")
@click.option("-w", "--workers", type=int, default=4, show_default=True, help="he number of server workers")
@click.option("--log-level", type=click.Choice(["debug", "info", "warning", "error"]), default="info",
              show_default=True, help="The granularity of log outputs")
@click.option("--gunicorn-config", type=str, show_default=True,
              help="Define the Gunicorn config file to be used, it will overide port and worker options")
def start_sage_server(config, port, workers, log_level, gunicorn_config):
    """Launch the Sage server using the CONFIG configuration file"""
    # check if config file exists
    if not isfile(config):
        print("Error: Configuration file not found: '{}'".format(config))
        print("Error: Sage server could not start, aborting...")
    else:
        if gunicorn_config is None:
            options = {
                'bind': '%s:%s' % ('0.0.0.0', port),
                'workers': workers,
                'log-level': log_level,
                'timeout': 120
            }
            StandaloneApplication(sage_app(config), options).run()
        else:
            # Load the default config file
            # => gunicorn_default_config.py
            spec = importlib.util.spec_from_file_location("sage_gunicorn_config", gunicorn_config)
            options = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(options)
            StandaloneApplication(sage_app(config), vars(options)).run()
