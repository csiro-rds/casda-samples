#!/usr/bin/env python -u

# Generate and download spectra of a list of locations. To see the command line usage help, use
# python get_spectra.py -h

from __future__ import print_function, division

from astropy.coordinates import SkyCoord
from astropy import units

import argparse
import casda
import os
import time


def parseargs():
    """
    Parse the command line arguments
    :return: An args map with the parsed arguments
    """
    parser = argparse.ArgumentParser(description="Generate and download spectra of a list of galaxies")
    parser.add_argument("opal_username",
                        help="Your user name on the ATNF's online proposal system (normally an email address)")
    parser.add_argument("-p", "--opal_password", help="Your password on the ATNF's online proposal system")
    parser.add_argument("--password_file", help="The file holding your password for the ATNF's online proposal system")
    parser.add_argument("coord_list", help="The file holding the list of positions, with one RA and Dec pair per line.")
    parser.add_argument("radius",
                        help="The radius in degrees to file holding the list of positions, with one RA and Dec pair per line.",
                        default=1.0)
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
    :param filename: The name of the file containing the list of sources
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


def build_pos_criteria(source_list, radius_degrees):
    """
    Build up a list of POS criteria that can be used in both SIA2 and SODA requests to specify locations with a
    radius. When used in SIA2 the positions will cause the results to be restricted to images and cubes which contain
    the specified areas. When used in SODA, the criteria specify individual regions to be extracted.

    :param source_list: The list of SkyCoord objects specifying source positions
    :param radius_degrees:  The number of degrees around the central point to extract or find.
    :return: An array of parameters for use with a POS keyword in either SIA2 or SODA.
    """
    pos_params = []
    for sky_loc in source_list:
        ra = sky_loc.ra.degree
        dec = sky_loc.dec.degree
        # print ra, dec, cutout_radius_degrees
        criterion = "CIRCLE " + str(ra) + " " + str(dec) + " " + str(radius_degrees)
        pos_params.append(criterion)
    return pos_params


def extract_spectra(source_list, cutout_radius_degrees, opal_username, opal_password, destination_directory):
    """
    Extract spectra at the specified locations from ASKAP image cubes. Only cubes of subtype spectral.restored.3d will be
    used in the extraction. Cubes that are either released or in a project that the opal user has pre-release access to
    will be included.

    :param source_list:  The list of SkyCoord objects specifying source positions
    :param cutout_radius_degrees: The number of degrees around the central point to extract or find.
    :param opal_username: The user's user name on the ATNF's online proposal system OPAL (normally an email address)
    :param opal_password: The user's OPAL password
    :param destination_directory: The directory where the resulting files will be stored
    :return: None
    """

    # Build query to produce list of cubes for the sources.
    pos_params = build_pos_criteria(source_list, cutout_radius_degrees)

    # Run an immediate sia2 job to get the list of target cubes
    votable = casda.find_images(pos_params, opal_username, opal_password)
    table = votable.get_first_table()
    authenticated_ids = []
    for row in table.array:
        # We are only interested in the restored spectral line cubes
        if row['dataproduct_subtype'] == 'spectral.restored.3d':
            data_product_id = row['obs_publisher_did']
            async_url, authenticated_id_token = casda.get_service_link_and_id(data_product_id, opal_username,
                                                                              opal_password,
                                                                              service='spectrum_generation_service',
                                                                              destination_dir=destination_directory)
            authenticated_ids.append(authenticated_id_token)

    if len(authenticated_ids) == 0:
        print('\n\nNo image cubes were found which matched any of your sources.')
        return

    # Generate spectra at each location for each cube - no spectra file is generated where there is no overlap
    job_location = casda.create_async_soda_job(authenticated_ids)
    casda.add_params_to_async_job(job_location, 'pos', pos_params)
    job_status = casda.run_async_job(job_location)
    print('\nJob finished with status %s address is %s\n\n' % (job_status, job_location))
    if job_status != 'ERROR':
        casda.download_all(job_location, destination_directory)


def main():
    args = parseargs()
    password = casda.get_opal_password(args.opal_password, args.password_file)

    # Change this to choose which environment to use, prod is the default
    casda.use_dev()

    start = time.time()
    if args.destination_directory is not None and not os.path.exists(args.destination_directory):
        os.makedirs(args.destination_directory)

    # Read the locations
    source_list = parse_sources_file(args.coord_list)

    # Do the work
    extract_spectra(source_list, args.radius, args.opal_username, password, args.destination_directory)

    # Report
    end = time.time()
    print('#### Spectra generation completed at %s ####'
          % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))))
    print('Full run took %.02f s' % (end - start))
    return 0


if __name__ == '__main__':
    exit(main())
