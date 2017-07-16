#############################################################################################
#
# Python script to demonstrate interaction with CASDA's SIAP v2 service.
# 
# This script does a SIA 2 query to get the image cubes for a given sky location, and creates an
# async job to download all matched image cube files.
#
# Author: James Dempsey on 30 Mar 2016
#
# Written for python 2.7
# Note: astropy is available on galaxy via 'module load astropy'
# On other machines, try Anaconda https://www.continuum.io/downloads
#
#############################################################################################

###### IMPORTS ######
# for reading commandline args
import sys
# for creating directory
import os
# regular expressions
import re
# sleep function
import time
# REST requests
import urllib, urllib2, base64
# XML parsing
import xml.etree.ElementTree as ET
# VO Table parsing
from astropy.io.votable import parse
from astropy.coordinates import SkyCoord
from astropy import units

# For hidden entry of password
import getpass

import casda

###### CONFIGURATION #######

if len(sys.argv) != 5 and len(sys.argv) != 6:
    print "Usage: siap.py OPAL_username [OPAL_password] ra dec Destination_Directory"
    sys.exit()

# your OPAL username and password
username = sys.argv[1] # First argument should be your OPAL username
if len(sys.argv) > 5:
    password = sys.argv[2] # Second argument should be your OPAL password
    argidx = 2
else:
    password = getpass.getpass("Enter your OPAL password: ")
    argidx = 1
# Sky location
ra = sys.argv[argidx+1] # Third argument should be right ascension of the centre of the sky region
dec = sys.argv[argidx+2] # Fourth argument should be declination of the centre of the sky region
if ra.find(':') > -1 or ra.find('h') > -1:
    sky_loc = SkyCoord(ra, dec, frame='icrs', unit=(units.hourangle, units.deg))
else:
    sky_loc = SkyCoord(ra, dec, frame='icrs', unit=(units.deg, units.deg))

search_radius_degrees = 0.1
dest_dir_root = sys.argv[argidx+3] # Fifth should be the root destination directory

# This query is used to select the image cubes for a given sky region
sky_region_query = 'CIRCLE %f %f %f' % (sky_loc.ra.degree, sky_loc.dec.degree, search_radius_degrees)

image_file_write_mode = 'wb' # needs to be 'wb' on windows
adql_file_write_mode = 'w' # ok for windows
destination_dir = dest_dir_root+"/" # directory the files will be saved to
poll_interval = 10 # number of seconds to wait between polls to check whether the async job has completed

casda_sia_query_base_url = "https://data.csiro.au/casda_vo_proxy/vo/sia2/query"
#casda_sia_query_base_url = "https://casdev.csiro.au/casda_vo_proxy/vo/sia2/query"
ns = {'uws':'http://www.ivoa.net/xml/UWS/v1.0'} # name space used to understand the XML job details response 

###### FUNCTIONS #######

def retrieve_data_link_to_file(image_cube_datalink_link_url, image_cube_id, username, password, destination_dir, file_write_mode):
    """ Read data link info for a given image cube to a file, returns the filename for this information """
    # Data link url for a given image cube
    request = urllib2.Request(image_cube_datalink_link_url)
    # Uses basic auth to securely access the data access information for the image cube
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)  
    
    # Save the data access vo table information to a file: eg C:/temp/datalink-cube-1234.xml
    result = urllib2.urlopen(request)
    data = result.read()
    filename = destination_dir + "datalink-" + image_cube_id + ".xml"
    with open(filename,file_write_mode) as f:
        f.write(data)
    return filename

def create_async_job(async_url, authenticated_id_tokens):
    """ Creates the async job, returning the url to query the job status and details """
    id_params = list(map((lambda authenticated_id_token: ('ID', authenticated_id_token)), authenticated_id_tokens))
    req = urllib2.Request(async_url)
    data = urllib.urlencode(id_params)
    print "Creating job: " + async_url+"?"+data
    u = urllib2.urlopen(req, data)
    #print u.read()
    return u.geturl()
    
def start_async_job(async_job_url):
    """ Start the async job """
    req = urllib2.Request(job_location+"/phase")
    data = urllib.urlencode({'phase':'RUN'})
    u = urllib2.urlopen(req, data)
    
def add_params_to_async_job(async_job_url, params):
    """ Add filter params the async job """
    req = urllib2.Request(job_location+"/parameters")
    data = urllib.urlencode(params)
    #print data
    u = urllib2.urlopen(req, data)
    print u.read()
    
def get_job_details_xml(async_job_url):
    """ Get job details as XML """
    req = urllib2.Request(async_job_url)
    u = urllib2.urlopen(req)
    job_response = u.read()
    #print job_response
    return ET.fromstring(job_response)
    
def read_job_status(job_details_xml, ns):
    """ Read job status from the job details XML """
    status = job_details_xml.find("uws:phase", ns).text
    return status

def get_error_message(job_details_xml, ns):
    """ Read job error message from the job details XML """
    errmsg = "None"
    errorSummary = job_details_xml.find("uws:errorSummary", ns)
    message = errorSummary.find("uws:message", ns)
    if (message is not None):
        errmsg = message.text
    return errmsg

def parse_datalink_for_authenticated_datalink_url(filename):
    """ Parses a datalink file into a vo table, and returns the authenticated datalink url """
    # Parse the datalink file into a vo table, and get the results
    votable = parse(filename, pedantic=False)
    results = next(resource for resource in votable.resources if resource.type == "results")
    if results == None:
        return None
    results_array = results.tables[0].array
    
    authenticated_datalink_url = None
    # Find the authenticated id token for accessing the image cube
    for x in results_array:
        if (x['description'] == "Authenticated Data Link"):
            authenticated_datalink_url = x['access_url']
    
    #print "Authenticated datalink url:", authenticated_datalink_url
                        
    return authenticated_datalink_url

def parse_datalink_for_service_and_id(filename, service_name):
    """ Parses a datalink file into a vo table, and returns the async service url and the authenticated id token """
    # Parse the datalink file into a vo table, and get the results
    votable = parse(filename, pedantic=False)
    results = next(resource for resource in votable.resources if resource.type == "results")
    if results == None:
        return None
    results_array = results.tables[0].array
    async_url = None
    authenticated_id_token = None

    # Find the authenticated id token for accessing the image cube
    for x in results_array:
        if (x['service_def'] == service_name):
            authenticated_id_token = x['authenticated_id_token']
            break
    
    # Find the async url  
    for x in votable.resources:
        if x.type == "meta":
            if (x.ID == service_name):
                for p in x.params:
                    if (p.name == "accessURL"):
                        async_url = p.value
                        break
                        
    #print "Async url:", async_url
    #print "Authenticated id token for async access:", authenticated_id_token
                        
    return async_url, authenticated_id_token
    

def sia2_query(url, pos_criteria, filename, username, password, file_write_mode):
    """ Do a sia v2 query, and write the resulting VO Table to a file """
    req = urllib2.Request(url)
    # Uses basic auth to securely access the data access information for the image cube
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    req.add_header("Authorization", "Basic %s" % base64string) 
    data = urllib.urlencode({'POS':pos_criteria})
    u = urllib2.urlopen(req, data)
    with open(filename,file_write_mode) as f:
        f.write(u.read())

###### SCRIPT ######

# 1) Create the destination directory
if not os.path.exists(destination_dir):
    os.makedirs(destination_dir)

# 2) Use CASDA SIA2 (secure) to query for the images associated with the given sky location
print "\n\n** Finding images and image cubes ... \n\n"
filename = destination_dir + "image_cubes.xml"
sia2_query(casda_sia_query_base_url, sky_region_query, filename, username, password, adql_file_write_mode)
#print(data_product_id_query)
image_cube_votable = parse(filename, pedantic=False)
results_array = image_cube_votable.get_table_by_id('results').array
#print results_array

# 3) For each of the image cubes, query datalink to get the secure datalink details 
print "\n\n** Retrieving datalink for each image and image cube...\n\n"
authenticated_id_tokens = []
async_url = None
image_cube_id = None
for image_cube_result in results_array:
    image_cube_datalink_url = image_cube_result['access_url']
    image_cube_id = image_cube_result['obs_publisher_did']
    #print image_cube_datalink_url, image_cube_id
    
    # 3a) Use datalink (may be secure or unsecure) to get the secure datalink details
    filename = retrieve_data_link_to_file(image_cube_datalink_url, image_cube_id, username, password, destination_dir, adql_file_write_mode)
    # If the obscore points to the unsecure datalink, this finds the secure datalink url
    authenticated_datalink_url = parse_datalink_for_authenticated_datalink_url(filename)
    # If the authenticated datalink url wasn't in the file, it means we went straight to the secure datalink details
    if authenticated_datalink_url != None:
        # This overwrites the file with the data from the secure datalink endpoint
        filename = retrieve_data_link_to_file(authenticated_datalink_url, image_cube_id, username, password, destination_dir, adql_file_write_mode)
    
    # 3b) Get the authenticated id tokens for the images, and the async request url
    async_url, authenticated_id_token = parse_datalink_for_service_and_id(filename, 'async_service')
    if authenticated_id_token != None:
        authenticated_id_tokens.append(authenticated_id_token)
    if async_url == None:
        print "WARNING: No async url found for image cube " + image_cube_id + ". You may not have been granted access to this image"

if len(authenticated_id_tokens) == 0:
    print "\n\nNo image cubes available in sky location %f %f" % (sky_loc.ra.degree, sky_loc.dec.degree)
    sys.exit()

# 4) Create the async job
job_location = create_async_job(async_url, authenticated_id_tokens)

# 6) Start the async job
print "\n\n** Starting the retrieval job...\n\n"
start_async_job(job_location)

# 7) Poll until the async job has finished
job_details = get_job_details_xml(job_location)
status = read_job_status(job_details, ns)
while status == 'EXECUTING' or status == 'QUEUED' or status == 'PENDING':
    print "Job %s, waiting for %d seconds." % (status, poll_interval)
    time.sleep(poll_interval)
    print "Polling job status" 
    job_details = get_job_details_xml(job_location)
    status = read_job_status(job_details, ns)
  
# 8) If the async job has completed successfully, it will download all of the files, otherwise will alert that it didn't complete 
if status == 'COMPLETED':
    print "\n\n** Downloading results...\n\n"
    for result in job_details.find("uws:results", ns).findall("uws:result", ns):
        casda.download_result_file(result, destination_dir=destination_dir)
else:
    print "Job did not complete: Status was %s with error: %s" % (status, get_error_message(job_details, ns))
