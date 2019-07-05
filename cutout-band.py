#!/usr/bin/env python -u

#############################################################################################
#
# Python script to demonstrate interacting with CASDA's TAP and SODA implementations to
# retrieve cutout images in bulk and slice them by frequency.
#
# This script does a TAP query to get the image cubes for a given scheduling block, and can be
# configured to either:
# a) conduct a second TAP query to identify catalogue entries of interest, and create an async
#    job to download cutouts at the RA and DEC of each of the catalogue entries.
# b) create an async job to download the entire image cube file.
#
# Author: Chris Trapani on 3 July 2019
#
# Written for python 2.7
# Note: astropy is available on galaxy via 'module load astropy'
# On other machines, try Anaconda https://www.continuum.io/downloads
#
#############################################################################################

from __future__ import print_function, division, unicode_literals

import argparse
import os

from astropy.io.votable import parse

import casda

def parseargs():
    """
    Parse the command line arguments
    :return: An args map with the parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Generate and download cutouts around each source identified in a scheduling block.")
    parser.add_argument("opal_username",
                        help="Your user name on the ATNF's online proposal system (normally an email address)")
    parser.add_argument("-p", "--opal_password", help="Your password on the ATNF's online proposal system")
    parser.add_argument("--password_file", help="The file holding your password for the ATNF's online proposal system")
    parser.add_argument("--full_files", help="Should full files be downloaded rather than just cutouts",
                        action='store_true')
    parser.add_argument("scheduling_block_id", help="The id of the ASKAP scheduling block to be queried.")
    parser.add_argument("channel_width", help="Number of channels to slice each cube by.")
    parser.add_argument("-type", "--data_product_type", help="Sub-type of the data product. E.g. 'spectral.restored.3d'.  "
                                                  "If not specified, a default value of 'spectral.restored.3d' "
                                                  "will be used.")
    parser.add_argument("destination_directory", help="The directory where the resulting files will be stored")

    args = parser.parse_args()
    return args


def download_cutouts(sbid, username, password, destination_dir, data_product_sub_type):
    # 2) Use CASDA VO (secure) to query for the images associated with the given scheduling_block_id
    print ("\n\n** Finding images and image cubes for scheduling block {} ... \n\n".format(sbid))

    #data_product_id_query = "select * from ivoa.obscore where obs_id = '" + str(
    #    sbid) + "' and dataproduct_type = 'cube' and dataproduct_subtype in ('cont.restored.t0', 'spectral.restored.3d')"

    data_product_id_query = "SELECT TOP 1000 * FROM ivoa.obscore where obs_publisher_did='cube-2008'"
    sbid_multi_channel_query = "SELECT TOP 1000 * FROM ivoa.obscore where obs_id='" + str(sbid) \
                               + "' and dataproduct_subtype='" + data_product_sub_type + "'"

    filename = destination_dir + "image_cubes_" + str(sbid) + ".xml"
    casda.sync_tap_query(data_product_id_query, filename, username, password)
    image_cube_votable = parse(filename, pedantic=False)
    results_array = image_cube_votable.get_table_by_id('results').array

    # 3) For each of the image cubes, query datalink to get the secure datalink details
    print ("\n\n** Retrieving datalink for each image and image cube...\n\n")
    authenticated_id_tokens = []
    for image_cube_result in results_array:
        image_cube_id = image_cube_result['obs_publisher_did'].decode('utf-8')
        async_url, authenticated_id_token = casda.get_service_link_and_id(image_cube_id, username,
                                                                          password,
                                                                          service='cutout_service',
                                                                          destination_dir=destination_dir)
        if authenticated_id_token is not None and len(authenticated_id_tokens) < 10:
            authenticated_id_tokens.append(authenticated_id_token)

    if len(authenticated_id_tokens) == 0:
        print ("No image cubes for scheduling_block_id " + str(sbid))
        return 1

    # For each source found in the catalogue query, create a position filter
    band_list = []
    for entry in results_array:
        v1 = '0.2101548491658654'
        v2 = '0.220026116656513'
        band = v1 + " " + v2;
        band_list.append(band)

    # Generate cutouts from each image around each source
    # where there is no overlap an error file is generated but can be ignored.
    job_location = casda.create_async_soda_job(authenticated_id_tokens)
    casda.add_params_to_async_job(job_location, 'BAND', band_list)
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
    casda.use_dev();

    data_product_sub_type = 'spectral.restored.3d' if not args.data_product_type else args.data_product_type

    return download_cutouts(args.scheduling_block_id, args.opal_username, password, destination_dir, data_product_sub_type)

if __name__ == '__main__':
    exit(main())
