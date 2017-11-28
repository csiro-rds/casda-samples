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

from __future__ import print_function, division

import argparse
import os

from astropy.coordinates import SkyCoord
from astropy import units

import casda


search_radius_degrees = 0.1


def parseargs():
    """
    Parse the command line arguments
    :return: An args map with the parsed arguments
    """
    parser = argparse.ArgumentParser(description="Download all image cube files for a given sky location")
    parser.add_argument("opal_username",
                        help="Your user name on the ATNF's online proposal system (normally an email address)")
    parser.add_argument("-p", "--opal_password", help="Your password on the ATNF's online proposal system")
    parser.add_argument("--password_file", help="The file holding your password for the ATNF's online proposal system")
    parser.add_argument("ra", help="The right ascension of the sky region")
    parser.add_argument("dec", help="The declination of the sky region")
    parser.add_argument("destination_directory", help="The directory where the resulting files will be stored")

    args = parser.parse_args()
    return args


def download_images(ra, dec, username, password, destination_dir):
    if ra.find(':') > -1 or ra.find('h') > -1:
        sky_loc = SkyCoord(ra, dec, frame='icrs', unit=(units.hourangle, units.deg))
    else:
        sky_loc = SkyCoord(ra, dec, frame='icrs', unit=(units.deg, units.deg))

    sky_region_query = 'CIRCLE %f %f %f' % (sky_loc.ra.degree, sky_loc.dec.degree, search_radius_degrees)

    # 2) Use CASDA SIA2 (secure) to query for the images associated with the given sky location
    print ("\n\n** Finding images and image cubes ... \n\n")
    image_cube_votable = casda.find_images([sky_region_query, ], username, password)
    results_array = image_cube_votable.get_table_by_id('results').array
    #print results_array

    # 3) For each of the image cubes, query datalink to get the secure datalink details
    print ("\n\n** Retrieving datalink for each image and image cube...\n\n")
    authenticated_id_tokens = []
    for image_cube_result in results_array:
        image_cube_id = image_cube_result['obs_publisher_did']
        async_url, authenticated_id_token = casda.get_service_link_and_id(image_cube_id, username,
                                                                          password,
                                                                          service='spectrum_generation_service',
                                                                          destination_dir=destination_dir)
        if authenticated_id_token is not None and len(authenticated_id_tokens) < 10:
            authenticated_id_tokens.append(authenticated_id_token)

    if len(authenticated_id_tokens) == 0:
        print ("\n\nNo image cubes available in sky location %f %f" % (sky_loc.ra.degree, sky_loc.dec.degree))
        return 1

    # 4) Create the async job
    job_location = casda.create_async_soda_job(authenticated_id_tokens)

    # 5) Run the async job
    print ("\n\n** Starting the retrieval job...\n\n")
    job_status = casda.run_async_job(job_location)
    print ('\nJob finished with status %s address is %s\n\n' % (job_status, job_location))
    if job_status != 'ERROR':
        casda.download_all(job_location, destination_dir)
    return 0


def main():
    args = parseargs()
    password = casda.get_opal_password(args.opal_password, args.password_file)

    # 1) Create the destination directory
    destination_dir = args.destination_directory + "/"
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    # Change this to choose which environment to use, prod is the default
    #casda.use_test()

    return download_images(args.ra, args.dec, args.opal_username, password, destination_dir)

if __name__ == '__main__':
    exit(main())
