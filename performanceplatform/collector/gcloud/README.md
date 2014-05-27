# Overview

Scrape a G-Cloud webpage to find a CSV, then download to a local database,
aggregate and publish to the performance platform.

The data is downloaded from
https://digitalmarketplace.blog.gov.uk/sales-accreditation-information/

# Sample data & tests

The `sample_data` directory contains:
- an example web-page that we scrape to get the CSV link
- an example CSV file of the sales data

The tests are written against these sample data.

# Data aggregation

- Load the raw sales spreadsheet into a SQLite database (using the scraperwiki
  python library) 

- Aggregate *number* of invoices & value of invoices for each unique month,
  year, lot, customer type, supplier sme/large and save to database table
  `aggregated_sales`

- Aggregate the *proportion* of invoices & proportion of value of invoices
  awarded to SMEs and large enterprises.

# Note on file encoding

Although we've tried to get G-Cloud to export the CSV with UTF8 encoding, this
has so far been unsuccessful and they are releasing as `cp1252` (I believe the
default Excel on Windows encoding)
