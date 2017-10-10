from __future__ import absolute_import, division, print_function

from zenlog import log as logger
import os
import yaml
import jinja2 as j2
import pkg_resources as pkg

def enum(*args):
    enums = dict(zip(args, range(len(args))))
    return type('Enum', (), enums)


Status = enum('PENDING', 'STARTED', 'FINISHED', 'FAILED')


def load_collections(collection):    
    return {
        ep.name: ep.load() 
        for ep in pkg.iter_entry_points(collection)
    }


class Config(object):
    __instance = None
    name = None

    # Singleton
    def __new__(cls):
        if Config.__instance is None:
            Config.__instance = object.__new__(cls)
            Config.__instance._cfg = os.environ.copy()
        return Config.__instance

    def load(self, stream):
        if os.path.exists(stream):
            with open(stream, 'r') as f:
                d = yaml.load(f)
        else:  # it's a yaml text string
            d = yaml.load(stream)
        self._cfg.update(d)

    def render_string(self, template_string):
        logger.info("Translating %s" % template_string)
        return j2.Template(template_string).render(self._cfg)

    def render(self, dct):
        if isinstance(dct, list):
            return [self.render(le) for le in dct]
        elif isinstance(dct, dict):
            return {
                k: self.render(v)
                for k, v in dct.iteritems()
            }
        elif isinstance(dct, str):
            return self.render_string(dct)
        else:
            return dct

    def update(self, dct):
        self._cfg.update(dct)
