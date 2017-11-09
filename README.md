# foursight #

A serverless chalice application to monitor and run tasks on [Fourfront](https://github.com/4dn-dcic/fourfront). Essentially, the app provides an number of endpoints to run checks, fetch results, dynamically create environments to check, and more.

[![Build Status](https://travis-ci.org/4dn-dcic/foursight.svg?branch=production)](https://travis-ci.org/4dn-dcic/foursight)
[![Coverage](https://coveralls.io/repos/github/4dn-dcic/foursight/badge.svg?branch=production)](https://coveralls.io/github/4dn-dcic/foursight?branch=production)

## Beta version

Foursight is under active development and features will likely change.

There are two foursight stages, dev and prod, each with their own API Gateway ID and address. To fetch latest results for all checks on a specific FF environment:
```
curl -X GET <foursight_address>/api/run/<environment>/all
```

To run all checks on a specific FF environment:
```
curl -X PUT <foursight_address>/api/run/<environment>/all
```

To run tests (from root level):
```
python test.py
```

Other endpoints are available and are being added as needed, so check back soon.