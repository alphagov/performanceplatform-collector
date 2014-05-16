
A python command line tool that aggregates data from third party sources and pushes
the result through to the Performance Platform (see http://alphagov.github.io/performanceplatform-documentation/
for more details). This tool will currently interface with Google Analytics
and Pingdom to retrieve data.

.. image:: https://travis-ci.org/alphagov/performanceplatform-collectors.svg
   :target: https://travis-ci.org/alphagov/performanceplatform-collectors

.. contents:: :local:

Installation
============

Using pip
---------

::

    $ pip install performanceplatform-collectors

From source
-----------

::

    $ git clone https://github.com/alphagov/performanceplatform-collectors.git
    $ cd performanceplatform-collectors
    $ virtualenv venv
    $ source venv/bin/activate
    $ python setup.py install

Usage
=====

::

    $ pp-collect -q [query_path] -c [credentials_path] -t [token_path] -b [backdrop_path]

There are also some optional command line arguments you can provide pp-collect::

    --console-logging
    Rather than logging out to log/collector.log it will output all logs to stdout/err

    --dry-run
    When it comes to submitting the gathered data to the Performance Platform it will skip
    making the POST requests and instead log out the url, headers and body to your terminal.

Configuration
-------------

There are four configuration files that get injected into pp-collect and are the four required
parameters:

- Query, contains everything about what the collector will do during execution. It provides an entrypoint
  that pp-collect will execute and provide the query and options k-v pairs::

      {
        "entrypoint": "performanceplatform.collector.pingdom",
        "query": {
          "name": "govuk"
        },
        "options": { },
        "data-set": {
          "data-group": "my-data-group",
          "data-set": "my-data-set"
        }
      }

- Token, this holds the bearer token to be used by this collector when POSTing to the Performance Platform::
  
      {
        "token": "some long hex value"
      }

- Credentials, passes through any usernames, passwords, API keys etc... that are required to communicate
  to the third party service you desire.
- Backdrop, where backdrop lives::
  
      {
        "url": "https://www.performance.service.gov.uk/data"
      }

For our deployment of performanceplatform-collectors we pull in deployment from performanceplatform-collectors-config_ repo. You
will find much more indepth information about the collector configuration in the repos README.

.. _performanceplatform-collectors-config: https://github.com/alphagov/performanceplatform-collectors-config

Entrypoints
===========

performanceplatform.collector.ga
--------------------------------

performanceplatform.collector.ga.trending
-----------------------------------------

performanceplatform.collector.ga.realtime
-----------------------------------------

performanceplatform.collector.pingdom
-------------------------------------

Extending performanceplatform-collectors
========================================
