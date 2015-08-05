.. _Google Analytics: http://www.google.com/analytics/
.. _Pingdom: https://www.pingdom.com/
.. _Webtrends: http://webtrends.com/

A python command line tool that aggregates data from third party sources and pushes
the result through to the Performance Platform (see http://alphagov.github.io/performanceplatform-documentation/
for more details). This tool uses the `Google Analytics`_, `Pingdom`_ and `Webtrends`_ APIs to retrieve data.

.. image:: https://travis-ci.org/alphagov/performanceplatform-collector.svg?branch=master
   :target: https://travis-ci.org/alphagov/performanceplatform-collector

.. image:: https://landscape.io/github/alphagov/performanceplatform-collector/master/landscape.png
   :target: https://landscape.io/github/alphagov/performanceplatform-collector/master
   :alt: Code Health

.. contents:: :local:


Installation
============

Using pip
---------

::

  pip install performanceplatform-collector

From source
-----------

::

  git clone https://github.com/alphagov/performanceplatform-collector.git
  cd performanceplatform-collector
  virtualenv venv
  source venv/bin/activate
  python setup.py install

Usage
=====

pp-collector takes paths to various JSON files as arguments::

  pp-collector -l [collector slug] -b [backdrop file] -c [credentials file] -t [token file]

All the target files are likely to be located in the performanceplatform-collector-config
repo. Make sure you update the content of the token file to match the token expected
by the Backdrop dataset.

There are also some optional command line arguments you can provide pp-collect::

    --console-logging
    Rather than logging out to log/collector.log it will output all logs to stdout/err

    --dry-run
    When it comes to submitting the gathered data to the Performance Platform it will skip
    making the POST requests and instead log out the url, headers and body to your terminal.

    --start, --end
    If you want the collector to gather past data, you can specify a start date in the format
    "YYYY-MM-DD". You must also specify an end date. e.g.

    --start=2014-08-03 --end=2014-09-03

Configuration
-------------

**Note on our configuation**

    For our deployment of the performanceplatform-collector we pull in configuration files from the performanceplatform-collector-config_ repo.
    The structure of our deployment configuration can be found there if more detailed examples are required.

.. _performanceplatform-collector-config: https://github.com/alphagov/performanceplatform-collector-config

There are four configuration files that get injected into pp-collector, each file is a required parameter.

Collector configuration
~~~~~~~~~~~~~~~~~~~~~~~
The collector configuration contains everything about what the collector will do during execution. It provides an entrypoint that pp-collector will execute and provide the query and options k-v pairs::

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


**A Note on Tokens**

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

  # Piwik example
  {
    "token_auth": "your Piwik secret token",
    "url": "your Piwik API url"
  }

  You can get your Piwik secret token from the Manage Users
  admin area in your Piwik account.

Google Analytics
================

Setting up Google Analytics credentials:
----------------------------------------

  .. image:: http://cl.ly/image/2W0M191L3L1O/Screen%20Shot%202014-06-10%20at%2011.11.21.png

To retrieve accurate paths for secrets (Google Analytics pathway):
  - Go to the `Google API Console <https://code.google.com/apis/console>`_ and create a new client ID (APIs & Auth > Credentials > OAuth > Create New Client ID)
  - Choose **installed application** > "other".
  - Once created click the Download JSON link. **This is your client secrets file.**
  - To generate the storage path you can run ``python tools/generate-ga-credentials.py path/to/client/secrets.json``

    + Follow the link to get the correct auth code
    + Copy and paste back into the CLI
    + This will default to creating google credentials in `./creds/ga.json`
    + **Error**::

      * If you get an 'invalid client error', adding a name and support email under the ""APIs & auth" -> "Consent screen" Should fix this.
      * See http://stackoverflow.com/questions/18677244/error-invalid-client-no-application-name for more.

Piwik
=====

Example Piwik configuration
---------------------------

Here is an example Piwik query file::

 {
   "data-set": {
      "data-group": "consular-appointment-booking-service",
      "data-type": "journey-by-goal"
    },
    "entrypoint": "performanceplatform.collector.piwik.core",
    "query": {
      "site_id": "9",
      "api_method": "Goals.get",
      "frequency": "daily",
      "api_method_arguments": {
         "idGoal": "3"
      }
    },
    "options": {
      "mappings": {
        "nb_visits_converted": "converted",
        "nb_conversions": "sessions"
      },
      "idMapping": ["dataType","_timestamp","timeSpan"]
      },
    "token": "piwik_fco"
 }

The above configuration will instruct the Piwik collector to fetch data
via the Goals.get method of your Piwik Reporting API endpoint. The
endpoint is specified via the 'url' setting in your credentials file.

The 'site_id' and 'frequency' settings map to the standard
Piwik Reporting API method arguments of 'idSite' and 'period' respectively.

* site_id - a number representing your website
* frequency - how statistics should be reported (daily, weekly, monthly)

If not specified, the 'frequency' setting defaults to 'weekly'.

You can specify API method-specific arguments using the 'api_method_arguments'
key in your query file as shown in the example. For a full list of methods
available in the Piwik Reporting API, see
http://developer.piwik.org/api-reference/reporting-api.

The Piwik collector uses the 'mappings' settings in your query file to determine
which data items to extract from an API response and how to map their
keys. The above query file, for example, will configure the collector
to extract the 'nb_visits_converted' and 'nb_conversions' data items
from the following example API response::

  {
    "From 2015-05-25 to 2015-05-31": {
      "nb_visits_converted": 791,
      "nb_conversions": 791,
      "conversion_rate": 18.09,
      "revenue": 0 }
  }

The keys of these data items will be replaced with
'converted' and 'sessions' respectively, ready for storage in
the Performance Platform's data application, Backdrop.

Running the Piwik collector
---------------------------

The Piwik collector is run from the command line in the normal
way - see the Usage section above.

If you want to collect data by day, week or month over a period of time,
specify an appropriate value for the 'frequency' setting in your
query file and a start and end date in your run command using the
'--start' and '--end' optional arguments. The dates are passed
to the Piwik API via a 'date' argument of the form 'YYYY-MM-DD,YYYY-MM-DD'.

If date arguments are not provided, a value of 'previous1' is passed
for the Piwik 'date' argument which will return data for the
previous day, week or month (according to the value of your
'frequency' setting).

Extending performanceplatform-collector
=======================================

performanceplatform-collector can be extended to support new types of
collector. To do so you'll need to add new entrypoints. For each new type of
collector create a file at::

    performanceplatform/collector/mycollectortype/__init__.py

Inside that file add a ``main`` function which has the following signature::

    main(credentials, data_set_config, query, options, start_at, end_at)

These arguments are all strings which are forwarded from the command line.

Developing performanceplatform-collector
========================================

To begin working on the code

::

  git clone https://github.com/alphagov/performanceplatform-collector.git
  cd performanceplatform-collector
  virtualenv venv
  source venv/bin/activate
  python setup.py develop

Due to the use of namespace packages, you must not install requirements with

::

  pip install -r requirements.txt

If you have run this command, your virtualenv may be broken - you can fix by
running

::

  pip uninstall performanceplatform-client
  python setup.py develop
