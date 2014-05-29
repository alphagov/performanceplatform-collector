
A python command line tool that aggregates data from third party sources and pushes
the result through to the Performance Platform (see http://alphagov.github.io/performanceplatform-documentation/
for more details). This tool will currently interface with Google Analytics
and Pingdom to retrieve data.

.. image:: https://travis-ci.org/alphagov/performanceplatform-collector.svg
   :target: https://travis-ci.org/alphagov/performanceplatform-collector

.. contents:: :local:

Installation
============

Using pip
---------

::

    $ pip install performanceplatform-collector

From source
-----------

::

    $ git clone https://github.com/alphagov/performanceplatform-collector.git
    $ cd performanceplatform-collector
    $ virtualenv venv
    $ source venv/bin/activate
    $ python setup.py install

Usage
=====

::

    $ pp-collect -q [query_path] -t [token_path] -b [backdrop_path]

There are also some optional command line arguments you can provide pp-collect::

    -c [credentials_path]
    Some entrypoints will require credentials in order to authenticate with the 3rd party
    services they pull data from. If it is required and you haven't provided a credentials
    file pp-collector will log an error and exit.

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

For our deployment of performanceplatform-collector we pull in deployment from performanceplatform-collector-config_ repo. You
will find much more indepth information about the collector configuration in the repos README.

.. _performanceplatform-collector-config: https://github.com/alphagov/performanceplatform-collector-config

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

Extending performanceplatform-collector
========================================
