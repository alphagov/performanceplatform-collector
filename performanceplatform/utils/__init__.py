import os

import statsd as _statsd


statsd = _statsd.StatsClient(
    prefix=os.getenv('GOVUK_STATSD_PREFIX', 'pp.apps.collector'))
