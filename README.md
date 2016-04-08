# CASDA Samples

This repository contains sample scripts which demonstrate interacting with CASDA VO services via programs.

Each script has extensive internal documentation and will return a usage string if run without parameters.

Detailed documentation on the how to use CASDA including how to run these scripts is available at
http://www.atnf.csiro.au/observers/data/casdaguide.html

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

### cutouts.py

Python script to demonstrate interacting with CASDA's TAP and SODA implementations to retrieve cutout images in bulk.

**Usage:** python cutouts.py OPAL_username OPAL_password scheduling_block_id Destination_Directory

This script does a TAP query to get the image cubes for a given scheduling block, and can be configured to either:

1. Conduct a second TAP query to identify catalogue entries of interest, and create an async job to download cutouts 
at the RA and DEC of each of the catalogue entries.
2. Create an async job to download the entire image cube file.

### siap.py

Python script to demonstrate interaction with CASDA's SIAP v2 service.

***Usage:*** python siap.py OPAL_username OPAL_password ra dec Destination_Directory 

This script does a SIA 2 query to get the image cubes for a given sky location, and creates an async job to download 
all matched image cube files.
