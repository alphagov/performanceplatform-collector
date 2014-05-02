
A python command line tool that aggregates data from third party sources and pushes
the result through to backdrop_. This tool will currently interface with Google Analytics
and Pingdom to retrieve data.

.. _backdrop: https://github.com/alphagov/backdrop

.. image:: https://travis-ci.org/alphagov/backdrop-collector.svg
   :target: https://travis-ci.org/alphagov/backdrop-collector

.. contents:: :local:

Installation
============

Using pip
---------

::

    $ pip install backdrop-collector

From source
-----------

::

    $ git clone https://github.com/alphagov/backdrop-collector.git
    $ cd backdrop-collector
    $ virtualenv venv
    $ source venv/bin/activate
    $ python setup.py install

Usage
=====

::

    $ backdrop-collector -q [query_path] -c [credentials_path] -t [token_path] -b [backdrop_path]

There are also some optional command line arguments you can provide backdrop_collector::

    --console-logging
    Rather than logging out to log/collector.log it will output all logs to stdout/err

    --dry-run
    When it comes to submitting the gathered data to backdrop it will skip making the POST requests
    and instead log out the url, headers and body to your terminal.

Configuration
-------------

There are four configuration files that get injected into backdrop-collector and are the four required
parameters:

- Query, contains everything about what the collector will do during execution. It provides an entrypoint
  that backdrop-collector will execute and provide the query and options k-v pairs::

      {
        "entrypoint": "backdrop.collector.pingdom",
        "query": {
          "name": "govuk"
        },
        "options": { },
        "data-set": {
          "data-group": "my-data-group",
          "data-set": "my-data-set"
        }
      }

- Token, this holds the bearer token to be used by this collector when POSTing to backdrop::
  
      {
        "token": "some long hex value"
      }

- Credentials, passes through any usernames, passwords, API keys etc... that are required to communicate
  to the third party service you desire.
- Backdrop, where backdrop lives::
  
      {
        "url": "https://www.performance.service.gov.uk/data"
      }

For our deployment of backdrop-collector we pull in deployment from backdrop-collector-config_ repo. You
will find much more indepth information about the collector configuration in the repos README.

.. _backdrop-collector-config: https://github.com/alphagov/backdrop-collector-config

Entrypoints
===========

backdrop.collector.ga
---------------------

backdrop.collector.ga.trending
------------------------------

backdrop.collector.ga.realtime
------------------------------

backdrop.collector.pingdom
--------------------------

Extending backdrop-collector
============================
