[![Build Status](https://travis-ci.org/NSLS-II/metadatastore.svg)](https://travis-ci.org/NSLS-II/metadatastore)
[![Coverage Status](https://coveralls.io/repos/NSLS-II/metadatastore/badge.svg?branch=master)](https://coveralls.io/r/NSLS-II/metadatastore?branch=master)
[![Code Health](https://landscape.io/github/NSLS-II/metadatastore/master/landscape.svg?style=flat)](https://landscape.io/github/NSLS-II/metadatastore/master)


# metadatastore
NSLS2 Beamlines metadatastore prototype implemented in MongoDB.

##Object Model
![metadatastore object model](https://github
.com/NSLS-IImetadatastore/doc/metadatastore_object_model.png)

##Description of the fields in each document
###BeamlineConfig
[Source code] (https://github
.com/NSLS-II/metadatastore/blob/master/metadatastore/odm_templates.py#L17)

The beamline configuration is intentionally ambiguous and is generally left 
up to the user to decide.  It is intended to be 
a bucket for any and all information that is constant across multiple 
runs/scans.  

An example of the kind of information that might be good to store in the 
`BeamlineConfig` document would be the distance from the sample to the 
detector, various detector parameters (size of pixels in um), and perhaps a 
pointer to the most recent calibration scan.  Such a `BeamlineConfig` might 
look like this:

```
BeamlineConfig
==============
calibration_scan_uid      : 6e78b971-30bb-4f48-8621-1ab95c2ccf94    
config_params             :
detector_dist             : 100                                     
detector_dist_units       : cm                                      
detector_pixel_size       : (100, 100)                              
detector_pixel_size_units : um                                      
time                      : 1433449177.4622374                      
time_as_datetime          : 2015-06-04 16:19:37.462237              
uid                       : 4c2d54a4-be2f-4b65-a520-695566fe4aa2    
```
