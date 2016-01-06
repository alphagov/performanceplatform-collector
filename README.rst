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

Using pip:

::

  pip install performanceplatform-collector

From source:

::

  git clone https://github.com/alphagov/performanceplatform-collector.git
  cd performanceplatform-collector
  virtualenv venv
  source venv/bin/activate
  python setup.py install

Usage
=====

Collectors are run from the command line using the Python progam pp-collector.

pp-collector takes paths to various JSON files as arguments::

  pp-collector (-l [collector slug] | -q [query file]) -b [backdrop file] -c [credentials file] -t [token file]

pp-collector also takes optional arguments::

  --console-logging
  Rather than logging out to log/collector.log it will output all logs to stdout/err

  --dry-run
  When it comes to submitting the gathered data to the Performance Platform it will skip
  making the POST requests and instead log out the url, headers and body to your terminal.

  --start, --end
  If you want the collector to gather past data, you can specify a start date in the format
  "YYYY-MM-DD". You must also specify an end date. e.g.

  --start=2014-08-03 --end=2014-09-03

Here's an example of how to run a collector from the command line using a collector slug argument::

  venv/bin/python /var/govuk/performanceplatform-collector/venv/bin/pp-collector -l performance-platform-devices-7abb3a26 -b performanceplatform.json -c ga-credentials.json -t ga-token.json --console-logging

pp-collector file path arguments
--------------------------------

**-l (collector slug)**

The collector slug is used to query Stagecraft to get collector configuration settings. The collector configuration is stored as a Python dictionary in the query_schema field on the Collector model identified by the given slug.

**-q (query file)**

A path to a query file can be used in place of the collector slug. The query file will contain the same collector configuration settings as would be stored in the Collector#query_schema field in Stagecraft. In addition, it must contain an entrypoint key whose value points to the type of collector to be run in the performanceplatform-collector codebase.

Here's an example of the contents of a query file::

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

Entrypoints describe a python package path.

.. _performanceplatform.collector.ga: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga
.. _performanceplatform.collector.ga.trending: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga/trending.py
.. _performanceplatform.collector.ga.realtime: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga/realtime.py
.. _performanceplatform.collector.gcloud: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/gcloud
.. _performanceplatform.collector.ga.trending: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/ga/trending.py
.. _performanceplatform.collector.pingdom: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/pingdom
.. _performanceplatform.collector.piwik: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/piwik
.. _performanceplatform.collector.piwik.core: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/piwik/core.py
.. _performanceplatform.collector.piwik.realtime: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/piwik/realtime.py
.. _performanceplatform.collector.webtrends: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/webtrends
.. _performanceplatform.collector.webtrends.keymetrics: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/webtrends/keymetrics.py
.. _performanceplatform.collector.webtrends.reports: https://github.com/alphagov/performanceplatform-collector/tree/master/performanceplatform/collector/webtrends/reports.py

The following entrypoints are currently available:

- `performanceplatform.collector.ga`_
- `performanceplatform.collector.ga.trending`_
- `performanceplatform.collector.ga.realtime`_
- `performanceplatform.collector.gcloud`_
- `performanceplatform.collector.pingdom`_
- `performanceplatform.collector.piwik`_
- `performanceplatform.collector.piwik.core`_
- `performanceplatform.collector.piwik.realtime`_
- `performanceplatform.collector.webtrends`_
- `performanceplatform.collector.webtrends.keymetrics`_
- `performanceplatform.collector.webtrends.reports`_

**-b (backdrop file)**

This is a simple pointer to the Performance Platform's data management application (Backdrop). It will define the endpoint for your collector to send all data to.

::

  {
    "backdrop_url": "https://www.performance.service.gov.uk/data",
    "stagecraft_url": "http://stagecraft.development.performance.service.gov.uk",
    "omniscient_api_token": "some-omniscient-token"
  }

stagecraft_url and omniscient_api_token token need only be defined when using the -l option to pass in a collector slug. The omniscient_api_token enables read-only access to the collector configuration settings stored in Stagecraft.

**-t (token file)**

The token file file holds the bearer token to be used by this collector when POSTing to the Performance Platform::

  {
    "token": "some long hex value"
  }

Make sure you update the content of the token file to match the token expected by the Backdrop data set being written to.

Need a token? Email The Performance Platform performance-platform@digital.cabinet-office.gov.uk

**-c (credentials file)**

The credentials file is used to pass through any usernames, passwords, API keys etc that are required to communicate to the third party service you desire.

::

  # Google Analytics example
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

  # Pingdom example
  {
      "user": "your Pingdom user",
      "password": "your Pingdom password",
      "app_key": "your Pingdom application key"
  }

  # WebTrends example
  {
      "user": "your WebTrends user",
      "password": "your WebTrends password",
      "reports_url": "your WebTrends report url",
      "keymetrics_url": "your WebTrends keymetrics url",
      "api_version": "your WebTrends API version e.g. v3"
  }

Setting up Google Analytics credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following steps will enable you to generate the credentials files that you will need to provide paths to in your credentials file (pointed to by the -c argument):

  - Go to the `Google API Console <https://code.google.com/apis/console>`_
  - Sign in to your Google account
  - If you don't have an active project, click on Create Project to create a new one. Give your project any name.
  - Create a new client ID

    + Go to **Use Google APIs**
    + Select **Credentials**
    + In the **New Credentials** drop-down list, select Oauth client ID.
    + If you see 'To create an OAuth client ID, you must first set a product name on the consent screen'; Configure your consent screen. The project name can be anything.
  - Choose **Application type** > "Other".
  - Enter a name. Again, the name can be anything
  - Once created click the download button. This will download a JSON file containing your client secrets.
  - To generate the storage path you run ``python tools/generate-ga-credentials.py path/to/client/secrets.json`` where secrets.json is the JSON file downloaded in the previous step.

    + The script will output a link to follow in Google accounts. Following the link to with generate an authorization code
    + Copy and paste the authorization code back into the CLI at the prompt.
    + Google credentials will be created in `./creds/ga.json`. The corresponding client_secrets.json and storage.db files will be created in `./creds/ga/`. You can point to these files in the credentials file referenced in the 'credentials file' argument.
    + **Error**::

      * If you get an 'invalid client error', adding a name and support email under the ""APIs & auth" -> "Consent screen" Should fix this.
      * See http://stackoverflow.com/questions/18677244/error-invalid-client-no-application-name for more.

About Piwik Collectors
======================

Example Piwik query file
------------------------

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

Developing performanceplatform-collector
========================================

Working on the code
-------------------

To begin working on the code::

  git clone https://github.com/alphagov/performanceplatform-collector.git
  cd performanceplatform-collector
  virtualenv venv
  source venv/bin/activate
  python setup.py develop

Due to the use of namespace packages, you must not install requirements with::

  pip install -r requirements.txt

If you have run this command, your virtualenv may be broken - you can fix by
running::

  pip uninstall performanceplatform-client
  python setup.py develop

Adding new types of collector
-----------------------------

performanceplatform-collector can be extended to support new types of
collector. To do so you'll need to add new entrypoints. For each new type of
collector create a file at::

    performanceplatform/collector/mycollectortype/__init__.py

Inside that file add a ``main`` function which has the following signature::

    main(credentials, data_set_config, query, options, start_at, end_at)

These arguments are all strings which are forwarded from the command line.
