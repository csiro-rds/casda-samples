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

from __future__ import print_function, division, unicode_literals

import argparse
import os

from astropy.io import votable
from astropy.coordinates import SkyCoord
from astropy import units

import casda


cutout_radius_degrees = 0.1  # The radius of the cutouts you want to generate


def parseargs():
    """
    Parse the command line arguments
    :return: An args map with the parsed arguments
    """
    parser = argparse.ArgumentParser(description="Download cutouts of specific locations from the specified image")
    parser.add_argument("opal_username",
                        help="Your user name on the ATNF's online proposal system (normally an email address)")
    parser.add_argument("-p", "--opal_password", help="Your password on the ATNF's online proposal system")
    parser.add_argument("--password_file", help="The file holding your password for the ATNF's online proposal system")
    parser.add_argument("image_id", help="The id of the target image.")
    parser.add_argument("source_list_file",
                        help="The file holding the list of positions, with one RA and Dec pair per line.")
    parser.add_argument("destination_directory", help="The directory where the resulting files will be stored")

    args = parser.parse_args()
    return args


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


def produce_cutouts(source_list, image_id, username, password, destination_dir):
    # Use CASDA VO (secure) to query for the images associated with the given scheduling_block_id
    print ("\n\n** Retreiving image details for %s ... \n\n" % image_id)
    filename = destination_dir + str(image_id) + ".xml"
    data_product_id_query = "select * from ivoa.obscore where obs_publisher_did = '" + image_id + \
                            "' and dataproduct_type = 'cube'"
    casda.sync_tap_query(data_product_id_query, filename, username=username, password=password)
    image_cube_votable = votable.parse(filename, pedantic=False)
    results_array = image_cube_votable.get_table_by_id('results').array

    # For each of the image cubes, query datalink to get the secure datalink details
    print ("\n\n** Retrieving datalink for each image and image cube...\n\n")
    authenticated_id_tokens = []
    for image_cube_result in results_array:
        image_cube_id = image_cube_result['obs_publisher_did'].decode('utf-8')
        async_url, authenticated_id_token = casda.get_service_link_and_id(image_cube_id, username,
                                                                          password,
                                                                          service='cutout_service',
                                                                          destination_dir=destination_dir)
        if authenticated_id_token is not None:
            authenticated_id_tokens.append(authenticated_id_token)

    if len(authenticated_id_tokens) == 0:
        print ("No image cubes found")
        return 1

    # Create the async job
    job_location = casda.create_async_soda_job(authenticated_id_tokens)

    # For each entry in the results of the catalogue query, add the position filter as a parameter to the async job
    cutout_filters = []
    for sky_loc in source_list:
        ra = sky_loc.ra.degree
        dec = sky_loc.dec.degree
        circle = "CIRCLE " + str(ra) + " " + str(dec) + " " + str(cutout_radius_degrees)
        cutout_filters.append(circle)
    casda.add_params_to_async_job(job_location, 'pos', cutout_filters)

    # Run the job
    status = casda.run_async_job(job_location)

    # Download all of the files, or alert if it didn't complete
    if status == 'COMPLETED':
        print ("\n\n** Downloading results...\n\n")
        casda.download_all(job_location, destination_dir)
    else:
        print ("Job did not complete: Status was %s." % status)
        return 1
    return 0


def main():
    args = parseargs()
    password = casda.get_opal_password(args.opal_password, args.password_file)

    # Change this to choose which environment to use, prod is the default
    # casda.use_dev()

    destination_dir = args.destination_directory + "/" + str(args.image_id) + "/"  # directory where files will be saved

    # 1) Read in the list of sources
    print ("\n\n** Parsing the source list ...\n")
    source_list = parse_sources_file(args.source_list_file)
    print ("\n** Read %d sources...\n\n" % (len(source_list)))

    # 2) Create the destination directory
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    # Do the work
    return produce_cutouts(source_list, args.image_id, args.opal_username, password, destination_dir)


if __name__ == '__main__':
    exit(main())
