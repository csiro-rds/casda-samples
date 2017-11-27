#!/usr/bin/env python -u

#############################################################################################
#
# Python script to demonstrate interacting with CASDA's TAP and SODA implementations to
# retrieve cutout images in bulk.
# 
# This script does a TAP query to get the image cubes for a given scheduling block, and can be
# configured to either:
# a) conduct a second TAP query to identify catalogue entries of interest, and create an async
#    job to download cutouts at the RA and DEC of each of the catalogue entries.
# b) create an async job to download the entire image cube file.
#
# Author: Amanda Helliwell on 16 Dec 2015
#
# Written for python 2.7
# Note: astropy is available on galaxy via 'module load astropy'
# On other machines, try Anaconda https://www.continuum.io/downloads
#
#############################################################################################

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
    parser.add_argument("destination_directory", help="The directory where the resulting files will be stored")

    args = parser.parse_args()
    return args


def download_cutouts(sbid, username, password, destination_dir, catalogue_query, do_cutouts, cutout_radius_degrees=0.1):
    # 2) Use CASDA VO (secure) to query for the images associated with the given scheduling_block_id
    print ("\n\n** Finding images and image cubes for scheduling block {} ... \n\n".format(sbid))
    data_product_id_query = "select * from ivoa.obscore where obs_id = '" + str(
        sbid) + "' and dataproduct_type = 'cube' and dataproduct_subtype in ('cont.restored.t0', 'spectral.restored.3d')"
    filename = destination_dir + "image_cubes_" + str(sbid) + ".xml"
    casda.sync_tap_query(data_product_id_query, filename, username, password)
    image_cube_votable = parse(filename, pedantic=False)
    results_array = image_cube_votable.get_table_by_id('results').array

    service = 'cutout_service' if do_cutouts else 'async_service'

    # 3) For each of the image cubes, query datalink to get the secure datalink details
    print ("\n\n** Retrieving datalink for each image and image cube...\n\n")
    authenticated_id_tokens = []
    for image_cube_result in results_array:
        image_cube_id = image_cube_result['obs_publisher_did']
        async_url, authenticated_id_token = casda.get_service_link_and_id(image_cube_id, username,
                                                                          password,
                                                                          service=service,
                                                                          destination_dir=destination_dir)
        if authenticated_id_token is not None and len(authenticated_id_tokens) < 10:
            authenticated_id_tokens.append(authenticated_id_token)

    if len(authenticated_id_tokens) == 0:
        print ("No image cubes for scheduling_block_id " + str(sbid))
        return 1

    # Run the catalogue_query to find catalogue entries that are of interest
    if do_cutouts:
        print ("\n\n** Finding components in each image and image cube...\n\n")
        filename = destination_dir + "catalogue_query_" + str(sbid) + ".xml"
        casda.sync_tap_query(catalogue_query, filename, username, password)
        catalogue_vo_table = parse(filename, pedantic=False)
        catalogue_results_array = catalogue_vo_table.get_table_by_id('results').array
        print ("\n\n** Found %d components...\n\n" % (len(catalogue_results_array)))
        if len(catalogue_results_array) == 0:
            print ("No catalogue entries matching the criteria found for scheduling_block_id " + str(sbid))
            return 1


        # For each source found in the catalogue query, create a position filter
        pos_list = []
        for entry in catalogue_results_array:
            ra = entry['ra_deg_cont']
            dec = entry['dec_deg_cont']
            circle = "CIRCLE " + str(ra) + " " + str(dec) + " " + str(cutout_radius_degrees)
            pos_list.append(circle)

    # Generate cutouts from each image around each source
    # where there is no overlap an error file is generated but can be ignored.
    job_location = casda.create_async_soda_job(authenticated_id_tokens)
    if do_cutouts:
        casda.add_params_to_async_job(job_location, 'pos', pos_list)
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
    # casda.use_at()


    catalogue_query = 'SELECT * FROM casda.continuum_component where first_sbid = {} and flux_peak > 500'.format(
        args.scheduling_block_id)

    return download_cutouts(args.scheduling_block_id, args.opal_username, password, destination_dir, catalogue_query,
                            not args.full_files)

if __name__ == '__main__':
    exit(main())
