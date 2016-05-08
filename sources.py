#############################################################################################
#
# Python script to demonstrate interacting with CASDA's SODA implementation to
# retrieve cutout images around a list of sources.
# 
# This script creates a job to produce and download cutouts from the specified image at
# the positions provided in an input file (each line has an RA and DEC).
#
# Author: James Dempsey on 16 Apr 2016
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

###### CONFIGURATION #######

if len(sys.argv) != 5 and len(sys.argv) != 6:
    print "Usage: sources.py OPAL_username [OPAL_password] image_id source_list_file Destination_Directory"
    sys.exit()

# your OPAL username and password
username = sys.argv[1] # First argument should be your OPAL username
if len(sys.argv) > 5:
    password = sys.argv[2] # Second argument should be your OPAL password
    argidx = 2
else:
    password = getpass.getpass("Enter your OPAL password: ")
    argidx = 1
# image and source info
image_id = sys.argv[argidx + 1] # Third should be the image_id, eg image-11 (can be obtained from a siap query)
source_list_file = sys.argv[argidx+2] # Fourth should be the file containing sources

dest_dir_root = sys.argv[argidx+3] # Fifth should be the root destination directory

# Initially this will take in an image id. After the 1.2 release it could take in an
# observation id and an image subtype

# This query is used to select the image cubes for a given scheduling block
#data_product_id_query = "select * from ivoa.obscore where obs_id = '"+ str(sbid) + "' and dataproduct_type = 'cube'"
data_product_id_query = "select * from ivoa.obscore where obs_publisher_did = '" + image_id + "' and dataproduct_type = 'cube'"

cutout_radius_degrees = 0.1 # The radius of the cutouts you want to generate

image_file_write_mode = 'wb' # needs to be 'wb' on windows
adql_file_write_mode = 'w' # ok for windows
destination_dir = dest_dir_root+"/"+str(image_id) + "/" # directory the files will be saved to
poll_interval = 10 # number of seconds to wait between polls to check whether the async job has completed

casda_adql_query_base_url = "https://data.csiro.au/casda_vo_proxy/vo/tap/sync"
#casda_adql_query_base_url = "https://castst.csiro.au/casda_vo_proxy/vo/tap/sync"
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
    
    # Find the authenticated id token for accessing the image cube
    for x in results_array:
        if (x['service_def'] == service_name):
            authenticated_id_token = x['authenticated_id_token']
    
    # Find the async url  
    for x in votable.resources:
        if x.type == "meta":
            if (x.ID == service_name):
                for p in x.params:
                    if (p.name == "accessURL"):
                        async_url = p.value

    #print "Async url:", async_url
    #print "Authenticated id token for async access:", authenticated_id_token
                        
    return async_url, authenticated_id_token
    
def download_result_file(result, file_write_mode):
    """ Downloads a result file, where input is an xml result entry from the async job response xml """
    file_location = urllib.unquote(result.get("{http://www.w3.org/1999/xlink}href")).decode('utf8')
    u = urllib2.urlopen(file_location)
    file_name = destination_dir + re.findall("filename=(\S+)", u.info().getheaders("Content-Disposition")[0])[0]
    print "Downloading from", file_location, "to", file_name
    with open(file_name,file_write_mode) as f:
        f.write(u.read())
    print "Download complete\n"

def adql_query(url, query_string, filename, username, password, file_write_mode):
    """ Do an adql query, and write the resulting VO Table to a file """
    req = urllib2.Request(url)
    # Uses basic auth to securely access the data access information for the image cube
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    req.add_header("Authorization", "Basic %s" % base64string)
    print req
    data = urllib.urlencode({'query':query_string, 'request':'doQuery', 'lang':'ADQL', 'format':'votable'})
    u = urllib2.urlopen(req, data)
    with open(filename,file_write_mode) as f:
        f.write(u.read())

def parse_sources_file(filename):
    """
    Read in a file of sources, with one source each line. Each source is specified as a
    right ascension and declination pair separated by space.
    e.g.
    1:34:56 -45:12:30
    320.20 -43.5
    :param filename: The name of the file contining the list of sources
    :return: A list of SkyCoord objects representing the parsed sources.
    """
    sourcelist = []
    with open(filename, 'r') as f:
        for line in f:
            if line and line[0] != '#':
                parts = line.split()
                if len(parts) > 1:
                    if parts[0].find(':') > -1 or parts[0].find('h') > -1:
                        sky_loc = SkyCoord(parts[0], parts[1], frame='icrs',
                                           unit=(units.hourangle, units.deg))
                    else:
                        sky_loc = SkyCoord(parts[0], parts[1], frame='icrs',
                                           unit=(units.deg, units.deg))
                    sourcelist.append(sky_loc)
    return sourcelist

###### SCRIPT ######

# 1) Read in the list of sources
print "\n\n** Parsing the source list ...\n"
source_list = parse_sources_file(source_list_file)
print "\n** Read %d sources...\n\n" % (len(source_list))

# 2) Create the destination directory
if not os.path.exists(destination_dir):
    os.makedirs(destination_dir)

# 3) Use CASDA VO (secure) to query for the images associated with the given scheduling_block_id
print "\n\n** Retreiving image details for %s ... \n\n" % (image_id)
filename = destination_dir + str(image_id) + ".xml"
adql_query(casda_adql_query_base_url, data_product_id_query, filename, username, password, adql_file_write_mode)
#print(data_product_id_query)
image_cube_votable = parse(filename, pedantic=False)
results_array = image_cube_votable.get_table_by_id('results').array
#print results_array

# 4) For each of the image cubes, query datalink to get the secure datalink details
print "\n\n** Retrieving datalink for each image and image cube...\n\n"
authenticated_id_tokens = []
async_url = None
image_cube_id = None
for image_cube_result in results_array:
    image_cube_datalink_url = image_cube_result['access_url']
    image_cube_id = image_cube_result['obs_publisher_did']
    #print image_cube_datalink_url, image_cube_id
    
    # 4a) Use datalink (may be secure or unsecure) to get the secure datalink details
    filename = retrieve_data_link_to_file(image_cube_datalink_url, image_cube_id, username, password, destination_dir, adql_file_write_mode)
    # If the obscore points to the unsecure datalink, this finds the secure datalink url
    authenticated_datalink_url = parse_datalink_for_authenticated_datalink_url(filename)
    # If the authenticated datalink url wasn't in the file, it means we went straight to the secure datalink details
    if authenticated_datalink_url != None:
        # This overwrites the file with the data from the secure datalink endpoint
        filename = retrieve_data_link_to_file(authenticated_datalink_url, image_cube_id, username, password, destination_dir, adql_file_write_mode)
    
    # 4b) Get the authenticated id tokens for the images, and the async request url
    async_url, authenticated_id_token = parse_datalink_for_service_and_id(filename, 'cutout_service')
    authenticated_id_tokens.append(authenticated_id_token)
    
    if async_url == None:
        print "No async url found for scheduling_block_id " + str(sbid) + " for image cube " + image_cube_id
        sys.exit()

if len(authenticated_id_tokens) == 0:
    print "No image cubes for scheduling_block_id " + str(sbid)
    sys.exit()

# 5) Create the async job
job_location = create_async_job(async_url, authenticated_id_tokens)
  
# 6) For each of the entries in the results of the catalogue query, add the position filter as a parameter to the async job
cutout_filters = []
for sky_loc in source_list:
    ra = sky_loc.ra.degree
    dec = sky_loc.dec.degree
    #print ra, dec, cutout_radius_degrees
    filter = "CIRCLE " + str(ra) + " " + str(dec) + " " + str(cutout_radius_degrees)
    cutout_filters.append(('pos',filter))
add_params_to_async_job(job_location, cutout_filters)

# 7) Start the async job
print "\n\n** Starting the retrieval job...\n\n"
start_async_job(job_location)

# 8) Poll until the async job has finished
job_details = get_job_details_xml(job_location)
status = read_job_status(job_details, ns)
while status == 'EXECUTING' or status == 'QUEUED' or status == 'PENDING':
    print "Job %s, waiting for %d seconds." % (status, poll_interval)
    time.sleep(poll_interval)
    print "Polling job status" 
    job_details = get_job_details_xml(job_location)
    status = read_job_status(job_details, ns)
  
# 9) If the async job has completed successfully, it will download all of the files, otherwise will alert that it didn't complete
if status == 'COMPLETED':
    print "\n\n** Downloading results...\n\n"
    for result in job_details.find("uws:results", ns).findall("uws:result", ns):
        download_result_file(result, image_file_write_mode)
else:
    print "Job did not complete: Status was %s with error: %s" % (status, get_error_message(job_details, ns))
