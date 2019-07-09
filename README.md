# CASDA Samples

This repository contains sample scripts which demonstrate interacting with CASDA VO services via programs.

Each script has extensive internal documentation and will return a usage string if run without parameters.

Detailed documentation on the how to use CASDA including how to run these scripts is available at
http://www.atnf.csiro.au/observers/data/casdaguide.html

![alt=Build status](https://img.shields.io/travis/csiro-rds/casda-samples.svg "Travis build status")

## OPAL Authentication

Many of the scripts require the use of credentials to access data, this is the case for all image and image cube access.

For general use, we recommend using OPAL accounts. To register with OPAL, go to the 
[OPAL Home Page](http://opal.atnf.csiro.au/) and click on the link to 'Register'. Enter your email address, name, 
affiliation and a password. The OPAL application will register you straight away and will then open a screen for you
to login.

OPAL user accounts are self-managed. Please keep your account details up to date. To change user-registration details, 
or to request a new OPAL password, use the links to 'Update your details' and 'Change your password'. If you have 
forgotten your password you may request that a new one be sent via email. 

## Script Details

Note: Where the OPAL_password is shown as optional if it is omitted the user will be prompted for it at run time.

### cutouts.py

Python script to demonstrate interacting with CASDA's TAP and SODA implementations to retrieve cutout images in bulk.

***Usage:*** python cutouts.py [-h] [-p OPAL_PASSWORD] [--password_file PASSWORD_FILE] [--full_files] opal_username scheduling_block_id destination_directory
                  
This script does a TAP query to get the image cubes for a given scheduling block, and can be configured to either:

1. Conduct a second TAP query to identify catalogue entries of interest, and create an async job to download cutouts 
at the RA and DEC of each of the catalogue entries.
2. Create an async job to download the entire image cube file.

### get_spectra.py

Python script to generate and download spectra of a list of locations. 

***Usage:*** python get_spectra.py [-h] [-p OPAL_PASSWORD] [--password_file PASSWORD_FILE] opal_username coord_list radius destination_directory

This script does a SIA 2 query to find image cubes including the given sources, and creates an async job to generate a spectrum for each source
and then download the spectra.


### siap.py

Python script to demonstrate interaction with CASDA's SIAP v2 service.

***Usage:*** python siap.py [-h] [-p OPAL_PASSWORD] [--password_file PASSWORD_FILE] opal_username ra dec destination_directory
               
This script does a SIA 2 query to get the image cubes for a given sky location, and creates an async job to download 
all matched image cube files.

### sources.py

Python script to demonstrate interaction with CASDA's SODA implementation to retrieve cutout images around a list of 
sources.

***Usage:*** python sources.py [-h] [-p OPAL_PASSWORD] [--password_file PASSWORD_FILE] opal_username image_id source_list_file destination_directory

This script does a TAP query to get the image cubes for a given scheduling block, and then produces cutouts for each
location in the source list file.

### cutout_channels.py

Python script to demonstrate interaction with CASDA's SODA implementation to retrieve image cubes for an observation and slice them by frequency.

***Usage:*** cutout_channels.py [-h] [-p OPAL_PASSWORD] [--password_file PASSWORD_FILE] [-type data_product_type] opal_username scheduling_block_id num_channels destination_directory

This script does a TAP query to get the image cubes for a given scheduling block, and then slices those image cubes by frequency.
Input field 'num_channels' specifies by many channels the image cubes will be sliced. 

E.g. if the cube has 1024 channels, specifying a 'num_channels' value of 512 will give two cubes each with 512 channels. 
