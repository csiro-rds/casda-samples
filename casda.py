# Library of useful functions for interacting with the CSIRO ASKAP Science Data Archive

# Author: James Dempsey

from __future__ import print_function, division, unicode_literals

import getpass
import os
import re
import time

from six.moves.urllib.parse import unquote


# VO Table parsing
from astropy.io.votable import parse
import requests
# XML parsing
from xml.etree import ElementTree

# name space used to understand the XML job details response
_uws_ns = {'uws': 'http://www.ivoa.net/xml/UWS/v1.0'}

_casda_base_url_vo_prod = "https://data.csiro.au/casda_vo_proxy/vo/"
_casda_base_url_vo_at = "https://daplt.csiro.au/casda_vo_proxy/vo/"
_casda_base_url_vo_test = "https://daptst.csiro.au/casda_vo_proxy/vo/"
_casda_base_url_vo_dev = "https://dapdev.csiro.au/casda_vo_proxy/vo/"

_casda_base_url_anon_vo_prod = "https://casda.csiro.au/casda_vo_tools/"
_casda_base_url_anon_vo_at = "https://casda-at-app.csiro.au/casda_vo_tools/"
_casda_base_url_anon_vo_test = "https://casda-tst-app.csiro.au/casda_vo_tools/"
_casda_base_url_anon_vo_dev = "https://casda-dev-app.csiro.au/casda_vo_tools/"

_casda_base_url_soda_prod = "https://casda.csiro.au/casda_data_access/"
_casda_base_url_soda_at = "https://casda-at-app.csiro.au/casda_data_access/"
_casda_base_url_soda_test = "https://casda-tst-app.csiro.au/casda_data_access/"
_casda_base_url_soda_dev = "https://casda-dev-app.csiro.au/casda_data_access/"

_casda_query_base_url = _casda_base_url_vo_prod
_casda_anon_query_base_url = _casda_base_url_anon_vo_prod
_casda_soda_base_url = _casda_base_url_soda_prod

_tap_sync_endpoint = "tap/sync"
_tap_async_endpoint = "tap/async"
_scs_endpoint = "scs"
_sia2_endpoint = "sia2/query"
_ssa_endpoint = "ssa"


def use_prod():
    """ Switch this CASDA library instance to use the production CASDA service. The prod instance is the default. """
    global _casda_query_base_url, _casda_anon_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_prod
    _casda_anon_query_base_url = _casda_base_url_anon_vo_prod
    _casda_soda_base_url = _casda_base_url_soda_prod


def use_at():
    """ Switch this CASDA library instance to use the acceptance test CASDA service. """
    global _casda_query_base_url, _casda_anon_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_at
    _casda_anon_query_base_url = _casda_base_url_anon_vo_at
    _casda_soda_base_url = _casda_base_url_soda_at


def use_test():
    """ Switch this CASDA library instance to use the test CASDA service. """
    global _casda_query_base_url, _casda_anon_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_test
    _casda_anon_query_base_url = _casda_base_url_anon_vo_test
    _casda_soda_base_url = _casda_base_url_soda_test


def use_dev():
    """ Switch this CASDA library instance to use the development CASDA service. """
    global _casda_query_base_url, _casda_anon_query_base_url, _casda_soda_base_url
    _casda_query_base_url = _casda_base_url_vo_dev
    _casda_anon_query_base_url = _casda_base_url_anon_vo_dev
    _casda_soda_base_url = _casda_base_url_soda_dev


def get_soda_async_url():
    return _casda_soda_base_url + "data/async"


def get_tap_async_url(proxy=True):
    """
    Retrieve the URL of the TAP async service.
    :param proxy: Should we use the authenticated proxy (defaults to true)
    :return: The URL of the async TAP service
    """
    if proxy:
        return _casda_query_base_url + _tap_async_endpoint
    else:
        return _casda_anon_query_base_url + _tap_async_endpoint


def get_tap_sync_url(proxy=True):
    """
    Retrieve the URL of the TAP sync service.
    :param proxy: Should we use the authenticated proxy (defaults to true)
    :return: The URL of the sync TAP service
    """
    if proxy:
        return _casda_query_base_url + _tap_sync_endpoint
    else:
        return _casda_anon_query_base_url + _tap_sync_endpoint


def get_datalink_url(dataproduct_id):
    return _casda_query_base_url + "datalink/links?ID=" + dataproduct_id


def create_async_soda_job(authenticated_id_tokens, soda_url=None):
    """ Creates the async job, returning the url to query the job status and details """
    id_params = list(
        map((lambda authenticated_id_token: ('ID', authenticated_id_token)),
            authenticated_id_tokens))
    async_url = soda_url if soda_url else get_soda_async_url()

    resp = requests.post(async_url, params=id_params)
    return resp.url


def sync_tap_query(query_string, filename, username=None, password=None,
                   file_write_mode='wb', tap_url=None):
    """
    Run an adql (TAP) query, and write the resulting VO Table to a file
    :param query_string: The ADQL query to be run
    :param filename: The name of the file where the query result should be saved.
    :param username: The OPAL username (if an authenticated query is required)
    :param password: The OPAL password (if an authenticated query is required)
    :param file_write_mode:  A string indicating how the file is to be opened (defaults to wb)
    :param tap_url: The URL of the TAP service, if a custom address is needed.
    :return: The path to the votable file
    """
    authenticated = password is not None
    sync_url = tap_url if tap_url else get_tap_sync_url(proxy=authenticated)

    params = {'query': query_string, 'request': 'doQuery', 'lang': 'ADQL', 'format': 'votable'}
    if authenticated:
        response = requests.get(sync_url, params=params, auth=(username, password))
    else:
        response = requests.get(sync_url, params=params)
    response.raise_for_status()
    with open(filename, file_write_mode) as f:
        f.write(response.content)
    return filename

def async_tap_query(query_string, username=None, password=None, destination_dir=None,
                    file_write_mode='wb', tap_url=None):
    """
    Run an adql (TAP) query, and write the resulting VO Table to a file
    :param query_string: The ADQL query to be run
    :param username: The OPAL username (if an authenticated query is required)
    :param password: The OPAL password (if an authenticated query is required)
    :param destination_dir: The directory where the files will be downloaded to. If not specified the files will be
            saved to the "temp" folder in the current directory.
    :param file_write_mode:  A string indicating how the file is to be opened (defaults to wb)
    :param tap_url: The URL of the TAP service, if a custom address is needed.
    :return: The path to the votable file
    """
    authenticated = password is not None
    async_url = tap_url if tap_url else get_tap_async_url(proxy=authenticated)

    params = {'query': query_string, 'lang': 'ADQL', 'format': 'votable'}
    if authenticated:
        response = requests.post(async_url, params=params, auth=(username, password))
    else:
        response = requests.post(async_url, params=params)
    job_url = response.url
    run_async_job(job_url)
    download_all(job_url, destination_dir, file_write_mode)
    return destination_dir + "result"

def create_async_tap_job(username=None, password=None, tap_url=None):
    """
    Creates the async Table Access Protocol job, returning the url to query the job status and details
    :param username: The OPAL username (if an authenticated query is required)
    :param password: The OPAL password (if an authenticated query is required)
    :param tap_url: The URL of the TAP service, if a custom address is needed.
    :return: The URL of the async job.
    """
    authenticated = password is not None
    async_url = tap_url if tap_url else get_tap_async_url(proxy=authenticated)

    print("Creating job: " + async_url)
    if authenticated:
        response = requests.post(async_url, auth=(username, password))
    else:
        response = requests.post(async_url)

    return response.url


def retrieve_direct_data_link_to_file(dataproduct_id,
                                      username, password,
                                      image_cube_datalink_link_url=None,
                                      destination_dir=None,
                                      file_write_mode='wb'):
    """ Read data link info for a given image cube to a file, returns the filename for this information """
    # Data link url for a given image cube
    url = get_datalink_url(dataproduct_id) if image_cube_datalink_link_url is None else image_cube_datalink_link_url
    print(url, image_cube_datalink_link_url)
    response = requests.get(url, auth=(username, password))
    response.raise_for_status()

    # Save the data access vo table information to a file: eg C:/temp/datalink-cube-1234.xml
    data = response.content
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
    if results is None:
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
    if authenticated_datalink_url is not None:
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
        if x['service_def'].decode("utf8") == service_name:
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


def add_param_to_async_job(job_location, param_key, param_value, verbose=False):
    """ Add filter params the async job """
    add_params_to_async_job(job_location, param_key, [param_value, ], verbose=False)


def add_params_to_async_job(job_location, param_key, param_values, verbose=False):
    """ Add multiple values for a filter parameter to the async job """
    params = list(map((lambda value: (param_key, value)), param_values))

    try:
        response = requests.post(job_location + "/parameters", data=params)
        response.raise_for_status()
        if verbose:
            print(response.text)
    except IOError as e:
        print("Unable to add %s parameters %s due to error %s" % (param_key, param_values, e))
        raise


def get_job_details_xml(async_job_url):
    """ Get job details as XML """
    response = requests.get(async_job_url)
    job_response = response.text
    return ElementTree.fromstring(job_response)


def read_job_status(job_details_xml, ns=_uws_ns):
    """ Read job status from the job details XML """
    status = job_details_xml.find("uws:phase", ns).text
    return status


def run_async_job(job_location, poll_interval=20):
    """
    Start an async job (e.g. TAP or SODA) and wait for it to be completed.

    :param job_location: The url to query the job status and details
    :param poll_interval: The number of seconds to wait between checks on the status of the job.
    :return: The single word status of the job. Normally COMPLETED or ERROR
    """

    # Start the async job
    print("\n\n** Starting the retrieval job...\n\n")
    response = requests.post(job_location + "/phase", data={'phase': 'RUN'})

    # Poll until the async job has finished
    job_details = get_job_details_xml(job_location)
    status = read_job_status(job_details)
    while status == 'EXECUTING' or status == 'QUEUED' or status == 'PENDING':
        print("Job %s, waiting for %d seconds." % (status, poll_interval))
        time.sleep(poll_interval)
        print("Polling job status")
        job_details = get_job_details_xml(job_location)
        status = read_job_status(job_details)
    return status

def run_async_jobs_and_download(job_locations, destination_dir, poll_interval=3):
    """
    Start many async jobs (e.g. TAP or SODA) in bulk and wait for it to be completed.
    Download will start when a job is completed

    :param job_locations: A list of urls to query each job for status / details
    :param destination_dir: Destination directory to download the data of the completed job.
    :param poll_interval: The number of seconds to wait between checks on the status of the job.
    """

    # start all jobs by using the /phase endpoint
    for job_location in job_locations:
        # Start the async job
        print("\n\n** Starting the retrieval job...\n\n")
        response = requests.post(job_location + "/phase", data={'phase': 'RUN'})

    # iterate each job and wait for completion
    # when complete start downloading
    jobs_completed = 0
    while jobs_completed != len(job_locations):
        for job_location in job_locations:
            job_details = get_job_details_xml(job_location)
            status = read_job_status(job_details)

            print("Job %s, waiting for %d seconds." % (status, poll_interval))
            time.sleep(poll_interval)
            print("Polling job status")
            job_details = get_job_details_xml(job_location)
            status = read_job_status(job_details)

            if(status == 'COMPLETED'):
                # finished job
                print('\nJob finished with status %s address is %s\n\n' % (status, job_location))
                jobs_completed += 1
                download_all(job_location, destination_dir)


def download_result_file(result, destination_dir=None, write_mode='wb'):
    """
    Downloads a result file, where input is an xml result entry from the async job response xml.

    :param result: The xml result entry specfying the details of an individual file.
    :param destination_dir: The directory where the file will be downloaded to. If not specified the file will be saved
            to the "temp" folder in the current directory.
    :param write_mode: The mode in which the file will be written.
    :return: The file name
    """
    file_location = unquote(result.get("{http://www.w3.org/1999/xlink}href"))
    response = requests.get(file_location, stream=True)
    if response.status_code != requests.codes.ok:
        if response.status_code == 404:
            print("Unable to download " + file_location)
            return None
        else:
            response.raise_for_status()

    name = list(filter(bool, file_location.split("/")))[-1]
    if 'Content-Disposition' in response.headers:
        header_cd = response.headers['Content-Disposition']
        if header_cd is not None and len(header_cd) > 0:
            result = re.findall('filename=(\S+)', header_cd[0])
            if result is not None and len(result) > 0:
                name = result[0]
    content_len = ""
    if 'Content-Length' in response.headers:
        content_len = str(response.headers['Content-Length']) + ' bytes'
    if destination_dir is None and not os.path.exists('temp'):
        os.makedirs('temp')

    file_name = ('temp' if destination_dir is None else destination_dir) + '/' + name
    print('Downloading {} from {} to {}'.format(content_len, file_location, file_name))
    block_size = 64 * 1024
    with open(file_name, write_mode) as f:
        for chunk in response.iter_content(chunk_size=block_size):
            f.write(chunk)
    print('Download complete\n')
    return file_name


def download_all(job_location, destination_dir=None, write_mode='wb'):
    """
    Download all result files from an async job (e.g. TAP or SODA).

    :param job_location: The url to query the job status and details
    :param destination_dir: The directory where the files will be downloaded to. If not specified the files will be
            saved to the "temp" folder in the current directory.
    :param write_mode: The mode in which the file will be written.
    :return: A list of the filenames downloaded
    """
    print("\n\n** Downloading results...\n\n")
    job_details = get_job_details_xml(job_location)
    filenames = []
    for result in job_details.find("uws:results", _uws_ns).findall("uws:result", _uws_ns):
        fn = download_result_file(result, destination_dir=destination_dir, write_mode=write_mode)
        if fn:
            filenames.append(fn)
    return filenames


def get_results_page(job_location):
    print(job_location)
    job_id = list(filter(bool, job_location.split("/")))[-1]
    return _casda_soda_base_url + "requests/" + job_id


def find_images(pos_criteria, username, password, maxrec=0):
    """
    Run an SIA2 query against CASDA to find images and cubes that contain any of the specified locations.
    See http://www.ivoa.net/documents/SIA/ for how to specify criteria.

    :param pos_criteria: An array of POS criteria (CIRCLE, POLYGON or RANGE) specifying the locations to be found.
    :param username: The OPAL username of the user.
    :param password: The OPAL password of the user.
    :param maxrec: The maximum number of images to retrieve, default is no limit
    :return: A VOTableFile object containing the SIA2 response. This will list the images along with extensive metadata.
    """
    url = _casda_query_base_url + _sia2_endpoint
    params = list(map((lambda value: ('POS', value)), pos_criteria))
    if maxrec > 0:
        params.append(('MAXREC', maxrec))
    response = requests.get(url, params=params, auth=(username, password))
    response.raise_for_status()
    if not os.path.exists('temp'):
        os.makedirs('temp')

    filename = 'temp/sia-resp.xml'
    with open(filename, 'wb') as f:
        f.write(response.content)
    votable = parse(filename, pedantic=False)
    return votable


def get_opal_password(opal_password, password_file):
    """
    Retrieve the OPAL password form the user, either from the command line arguments, the file they specified or
      by asking them to input it
    :param opal_password: The actual password, if provided via the command line
    :param password_file: The file containing the password, if provided.
    :return: The password
    """
    if opal_password:
        return opal_password

    if password_file:
        with open(password_file, 'r') as fd:
            password = fd.readlines()[0].strip()
    else:
        password = getpass.getpass(str("Enter your OPAL password: "))

    return password
