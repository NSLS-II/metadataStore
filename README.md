[![Build Status](https://travis-ci.org/NSLS-II/metadatastore.svg)](https://travis-ci.org/NSLS-II/metadatastore)
[![Coverage Status](https://coveralls.io/repos/NSLS-II/metadatastore/badge.svg?branch=master)](https://coveralls.io/r/NSLS-II/metadatastore?branch=master)
[![Code Health](https://landscape.io/github/NSLS-II/metadatastore/master/landscape.svg?style=flat)](https://landscape.io/github/NSLS-II/metadatastore/master)


# metadatastore

[![Join the chat at https://gitter.im/NSLS-II/metadatastore](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/NSLS-II/metadatastore?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
NSLS2 Beamlines metadatastore prototype implemented in MongoDB.

## Conda Recipes

Install the most recent tagged build: `conda install filestore -c lightsource2-tag`

Install the most recent tagged build: `conda install filestore -c lightsource2-dev`

Find the tagged recipe [here](https://github.com/NSLS-II/lightsource2-recipes/tree/master/recipes-tag/filestore) and the dev recipe [here](https://github.com/NSLS-II/lightsource2-recipes/tree/master/recipes-dev/filestore)


## metadatastore configuration

Metadatastore requires the following configuration information:

```python
database: metadatastore
port: 27017
host: 127.0.0.1
timezone: US/Eastern
```

where

 - `metadatastore` can be any valid string
 - `127.0.0.1` can be any IP/DNS name
 - `US/Eastern` can be any [of these timezone strings] (https://www.vmware.com/support/developer/vc-sdk/visdk400pubs/ReferenceGuide/timezone.html)

This configuration information can live in up to four different places, as
defined in the docstring of the `load_configuration` function in
 `metadatastore/conf.py`. In order of increasing precedence:

1. The conda environment
  - CONDA_ENV/etc/{name}.yaml (if CONDA_ETC_env is defined)
1. At the system level
  - /etc/{name}.yml
1. In the user's home directory
  - ~/.config/{name}/connection.yml
1. Environmental variables
  - {PREFIX}_{FIELD}

where

  - {name} is metadatastore
  - {PREFIX} is MDS and {FIELD} is one of {host, database, port, timezone}
