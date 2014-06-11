.. _Google Analytics: http://www.google.com/analytics/
.. _Pingdom: https://www.pingdom.com/

A python command line tool that aggregates data from third party sources and pushes
the result through to the Performance Platform (see http://alphagov.github.io/performanceplatform-documentation/
for more details). This tool will currently interface with `Google Analytics`_
and `Pingdom`_ to retrieve data.

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

pp-collector takes paths to various JSON files as arguments::

  $ pp-collector -q [query file] -b [backdrop file] -c [credentials file] -t [token file]

There are also some optional command line arguments you can provide pp-collector::

    --console-logging
    Rather than logging out to log/collector.log it will output all logs to stdout/err

    --dry-run
    When it comes to submitting the gathered data to the Performance Platform it will skip
    making the POST requests and instead log out the url, headers and body to your terminal.

Configuration
-------------

::

    For our deployment of the performanceplatform-collector we pull in configuration files from the performanceplatform-collector-config_ repo.
    The structure of our deployment configuration can be found there if more detailed examples are required.

.. _performanceplatform-collector-config: https://github.com/alphagov/performanceplatform-collector-config

There are four configuration files that get injected into pp-collector, each file is a required parameter.

Query File
~~~~~~~~~~
The query file contains everything about what the collector will do during execution. It provides an entrypoint that pp-collector will execute and provide the query and options k-v pairs::

  # pingdom example
  {
    "entrypoint": "performanceplatform.collector.pingdom",
    "query": {
      "name": "govuk"
    },
    "options": {
      "additionalFields": {
        # Every record sent to backdrop will have these additional fields
        "foo": "bar",
        "sentAt": "specific-formatted-time-value"
      }
    },
    "plugins": [
      "Comment('Pingdom stats are aggregated using mycustomtemplate.py')
    ],
    "data-set": {
      "data-group": "my-data-group",
      "data-set": "my-data-set"
    }
  }

**Entrypoints:**

Entrypoints describe a python package path

The following entrypoints are currently available::

`performanceplatform.collector.ga`_
`performanceplatform.collector.ga.trending`_
`performanceplatform.collector.ga.realtime`_
`performanceplatform.collector.pingdom`_

 .. _performanceplatform.collector.ga: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga
 .. _performanceplatform.collector.ga.trending: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga/trending.py
 .. _performanceplatform.collector.ga.realtime: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga/realtime.py
 .. _performanceplatform.collector.pingdom: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/pingdom

Backdrop file
~~~~~~~~~~~~~

This is a simple pointer to the performance platform's data-store application. It will define the endpoint for your collector to send all data to.::

  {
    "url": "https://www.performance.service.gov.uk/data"
  }


Token File
~~~~~~~~~~

::

  Need a token? Email The Performance Platform performance-platform@digital.cabinet-office.gov.uk

The token file file holds the bearer token to be used by this collector when POSTing to the Performance Platform::

  {
    "token": "some long hex value"
  }

Credentials file
~~~~~~~~~~~~~~~~
The credentials file is used to pass through any usernames, passwords, API keys etc that are required to communicate to the third party service you desire.::

  # Google analytics Specific example
  credentials = {
      "CLIENT_SECRETS": path/to/client_secret.json,
      "STORAGE_PATH": path/to/oauth/db,
  }

Google Analytics
================

Setting up Google Analytics Credentials:
----------------------------------------

  .. image:: http://cl.ly/image/2W0M191L3L1O/Screen%20Shot%202014-06-10%20at%2011.11.21.png

To retrieve accurate paths for secrets (google analytics pathway):
  - Go to the `Google API Console <https://code.google.com/apis/console>`_ and create a new client ID (APIs & Auth > Credentials > OAuth > Create New Client ID)
  - Choose **installed application** > "other".
  - Once created click the Download JSON link. **This is your client secrets file.**
  - To generate the storage path you can run ``https://github.com/alphagov/performanceplatform-collector/blob/master/tools/generate-ga-credentials.py path/to/client/secrets.json``

    + Follow the link to get the correct auth code
    + Copy and paste back into the CLI
    + This will default to creating google credentials in `./creds/ga.json`
    + **Error**::

      * If you get an 'invalid client error', adding a name and support email under the ""APIs & auth" -> "Consent screen" Should fix this.
      * See http://stackoverflow.com/questions/18677244/error-invalid-client-no-application-name for more.



Extending performanceplatform-collector
=======================================

performanceplatform-collector can be extended to support new types of
collector. To do so you'll need to add new entrypoints. For each new type of
collector create a file at::

    performanceplatform/collector/mycollectortype/__init__.py

Inside that file add a ``main`` function which has the following signature::

    main(credentials, data_set_config, query, options, start_at, end_at)

These arguments are all strings which are forwarded from the command line.

