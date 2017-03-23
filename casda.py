# Library of useful functions for interacting with the CSIRO ASKAP Science Data Archive

# Author: James Dempsey

from __future__ import print_function, division

import base64
import re
import time
import urllib, urllib2


# VO Table parsing
from astropy.io.votable import parse
# XML parsing
import xml.etree.ElementTree as ET

# name space used to understand the XML job details response
_uws_ns = {'uws':'http://www.ivoa.net/xml/UWS/v1.0'}


_casda_base_url_vo_prod = "https://data.csiro.au/casda_vo_proxy/vo/"
_casda_base_url_vo_at = "https://daplt.csiro.au/casda_vo_proxy/vo/"
_casda_base_url_vo_test = "https://castst.csiro.au/casda_vo_proxy/vo/"
_casda_base_url_vo_dev = "https://casdev.csiro.au/casda_vo_proxy/vo/"

_casda_base_url_soda_prod = "https://casda.csiro.au/casda_data_access/"
_casda_base_url_soda_at = "https://casda-a-app.pawsey.org.au/casda_data_access/"
_casda_base_url_soda_test = "https://casda-t-app.pawsey.org.au/casda_data_access/"
_casda_base_url_soda_dev = "https://casda-dev-app.pawsey.org.au/casda_data_access/"

_casda_query_base_url = _casda_base_url_vo_prod
_casda_soda_base_url = _casda_base_url_soda_prod

_tap_sync_endpoint = "tap/sync"
_scs_endpoint = "scs"
_sia2_endpoint = "sia2/query"
_ssa_endpoint = "ssa"


def use_prod():
    """ Switch this CASDA library instance to use the production CASDA service. The prod instance is the default. """
    global _casda_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_prod
    _casda_soda_base_url = _casda_base_url_soda_prod


def use_at():
    """ Switch this CASDA library instance to use the acceptance test CASDA service. """
    global _casda_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_at
    _casda_soda_base_url = _casda_base_url_soda_at


def use_test():
    """ Switch this CASDA library instance to use the test CASDA service. """
    global _casda_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_test
    _casda_soda_base_url = _casda_base_url_soda_test


def use_dev():
    """ Switch this CASDA library instance to use the development CASDA service. """
    global _casda_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_dev
    _casda_soda_base_url = _casda_base_url_soda_dev


def get_soda_async_url():
    return _casda_soda_base_url + "soda/async"


def get_datalink_url(dataproduct_id):
    return _casda_query_base_url + "datalink/links?ID=" + dataproduct_id


def create_async_soda_job(authenticated_id_tokens, soda_url=None):
    """ Creates the async job, returning the url to query the job status and details """
    id_params = list(
        map((lambda authenticated_id_token: ('ID', authenticated_id_token)),
            authenticated_id_tokens))
    async_url = soda_url if soda_url else get_soda_async_url()

    req = urllib2.Request(async_url)
    data = urllib.urlencode(id_params)
    print ("Creating job: " + async_url + "?" + data)
    u = urllib2.urlopen(req, data)
    # print u.read()
    return u.geturl()


def retrieve_direct_data_link_to_file(dataproduct_id,
                                      username, password,
                                      image_cube_datalink_link_url=None,
                                      destination_dir=None,
                                      file_write_mode='wb'):
    """ Read data link info for a given image cube to a file, returns the filename for this information """
    # Data link url for a given image cube
    url = get_datalink_url(dataproduct_id) if image_cube_datalink_link_url is None else image_cube_datalink_link_url
    print (url, image_cube_datalink_link_url)
    request = urllib2.Request(url)
    # Uses basic auth to securely access the data access information for the image cube
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)

    # Save the data access vo table information to a file: eg C:/temp/datalink-cube-1234.xml
    result = urllib2.urlopen(request)
    data = result.read()
    filename = destination_dir + "/datalink-" + dataproduct_id + ".xml"
    with open(filename, file_write_mode) as f:
        f.write(data)
    return filename


def parse_datalink_for_authenticated_datalink_url(filename):
    """ Parses a datalink file into a vo table, and returns the authenticated datalink url """
    # Parse the datalink file into a vo table, and get the results
    votable = parse(filename, pedantic=False)
    results = next(resource for resource in votable.resources if
                   resource.type == "results")
    if results == None:
        return None
    results_array = results.tables[0].array

    authenticated_datalink_url = None
    # Find the authenticated id token for accessing the image cube
    for x in results_array:
        if x['description'] == "Authenticated Data Link":
            authenticated_datalink_url = x['access_url']

    # print "Authenticated datalink url:", authenticated_datalink_url
    return authenticated_datalink_url


def retrieve_data_link_to_file(dataproduct_id,
                               username, password,
                               image_cube_datalink_link_url=None,
                               destination_dir=None,
                               file_write_mode='wb'):
    # 3a) Use datalink (may be secure or unsecure) to get the secure datalink details
    filename = retrieve_direct_data_link_to_file(dataproduct_id, username,
                                                 password,
                                                 image_cube_datalink_link_url=image_cube_datalink_link_url,
                                                 destination_dir=destination_dir,
                                                 file_write_mode=file_write_mode)
    # If the obscore points to the unsecure datalink, this finds the secure datalink url
    authenticated_datalink_url = parse_datalink_for_authenticated_datalink_url(
        filename)
    # If the authenticated datalink url wasn't in the file, it means we went straight to the secure datalink details
    if authenticated_datalink_url != None:
        # This overwrites the file with the data from the secure datalink endpoint
        filename = retrieve_direct_data_link_to_file(dataproduct_id, username,
                                                     password,
                                                     image_cube_datalink_link_url=authenticated_datalink_url,
                                                     destination_dir=destination_dir,
                                                     file_write_mode=file_write_mode)

    return filename


def parse_datalink_for_service_and_id(filename, service_name):
    """ Parses a datalink file into a vo table, and returns the async service url and the authenticated id token """
    # Parse the datalink file into a vo table, and get the results
    votable = parse(filename, pedantic=False)
    results = next(resource for resource in votable.resources if
                   resource.type == "results")
    if results is None:
        return None
    results_array = results.tables[0].array
    async_url = None
    authenticated_id_token = None

    # Find the authenticated id token for accessing the image cube
    for x in results_array:
        if x['service_def'] == service_name:
            authenticated_id_token = x['authenticated_id_token']

    # Find the async url
    for x in votable.resources:
        if x.type == "meta":
            if x.ID == service_name:
                for p in x.params:
                    if p.name == "accessURL":
                        async_url = p.value

    # print "Async url:", async_url
    # print "Authenticated id token for async access:", authenticated_id_token

    return async_url, authenticated_id_token


def get_service_link_and_id(dataproduct_id,
                               username, password,
                               image_cube_datalink_link_url=None,
                               destination_dir=None,
                               file_write_mode='wb',
                            service='cutout_service'):
    filename = retrieve_data_link_to_file(dataproduct_id,
                                          username,
                                          password,
                                          image_cube_datalink_link_url=image_cube_datalink_link_url,
                                          destination_dir=destination_dir,
                                          file_write_mode=file_write_mode)
    return parse_datalink_for_service_and_id(filename, service)


def add_params_to_async_job(job_location, param_key, param_values, verbose=False):
    """ Add filter params the async job """
    params = list(map((lambda value: (param_key, value)), param_values))

    req = urllib2.Request(job_location+"/parameters")
    data = urllib.urlencode(params)
    #print data
    try:
        u = urllib2.urlopen(req, data)
        if verbose:
            print (u.read())
    except IOError as e:
        print ("Unable to add %s parameters %s due to error %s" % (param_key, param_values, e))
        raise


def get_job_details_xml(async_job_url):
    """ Get job details as XML """
    req = urllib2.Request(async_job_url)
    u = urllib2.urlopen(req)
    job_response = u.read()
    # print job_response
    return ET.fromstring(job_response)


def read_job_status(job_details_xml, ns=_uws_ns):
    """ Read job status from the job details XML """
    status = job_details_xml.find("uws:phase", ns).text
    return status


def run_async_job(job_location, poll_interval=20):
    # 6) Start the async job
    print ("\n\n** Starting the retrieval job...\n\n")
    req = urllib2.Request(job_location+"/phase")
    data = urllib.urlencode({'phase':'RUN'})
    u = urllib2.urlopen(req, data)

    # 7) Poll until the async job has finished
    job_details = get_job_details_xml(job_location)
    status = read_job_status(job_details)
    while status == 'EXECUTING' or status == 'QUEUED' or status == 'PENDING':
        print ("Job %s, waiting for %d seconds." % (status, poll_interval))
        time.sleep(poll_interval)
        print ("Polling job status")
        job_details = get_job_details_xml(job_location)
        status = read_job_status(job_details)
    return status

def download_result_file(result, destination_dir=None, write_mode='wb'):
    """ Downloads a result file, where input is an xml result entry from the async job response xml """
    file_location = urllib.unquote(result.get("{http://www.w3.org/1999/xlink}href")).decode('utf8')
    u = urllib2.urlopen(file_location)
    name = filter(bool, file_location.split("/"))[-1]
    header_cd = u.info().getheaders("Content-Disposition")
    if not header_cd == None and len(header_cd) > 0:
        result = re.findall("filename=(\S+)", header_cd[0])
        if not result == None and len(result) > 0:
            name = result[0]
    file_name = "temp" if destination_dir is None else destination_dir + "/" + name
    print ("Downloading from", file_location, "to", file_name)
    with open(file_name, write_mode) as f:
        f.write(u.read())
    print ("Download complete\n")


def download_all(job_location, destination_dir=None, write_mode='wb'):
    print ("\n\n** Downloading results...\n\n")
    job_details = get_job_details_xml(job_location)
    for result in job_details.find("uws:results", _uws_ns).findall("uws:result", _uws_ns):
        download_result_file(result, destination_dir=destination_dir, write_mode=write_mode)


def get_results_page(job_location):
    print (job_location)
    job_id = filter(bool, job_location.split("/"))[-1]
    return _casda_soda_base_url + "requests/" + job_id