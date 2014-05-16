"""
load_plugin.py
--------------

Responsible for taking plugin strings and returning plugin callables.

"""

# For the linter
import __builtin__

import performanceplatform.collector.ga.plugins


def load_plugins(plugin_names):
    return [load_plugin(plugin_name) for plugin_name in plugin_names]


def load_plugin(plugin_name):

    expr = compile(plugin_name, "performanceplatform.collector plugin", "eval")

    return eval(expr, __builtin__.__dict__,
                performanceplatform.collector.ga.plugins.__dict__)
