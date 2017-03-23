#!/usr/bin/env python -u

# Load testing script to test out producing large numbers of cutouts from single large cube

from __future__ import print_function, division

import argparse
import casda
import getpass
import json
import numpy as np
import os
import time

_large_cutout_s_len = 100
#_medium_cutout_s_len = 40
_small_cutout_s_len = 20
_large_cutout_em_len = 60
#_medium_cutout_em_len = 30
_small_cutout_em_len = 10
_c = 2.9979e8


def parseargs():
    """
    Parse the command line arguments
    :return: An args map with the parsed arguments
    """
    parser = argparse.ArgumentParser(description="Produce some random cutouts from a large image cube")
    parser.add_argument("opal_username",
                        help="Your user name on the ATNF's online proposal system (normally an email address)")
    parser.add_argument("-p", "--opal_password", help="Your password on the ATNF's online proposal system")
    parser.add_argument("--passwordfile", help="The file holding your password for the ATNF's online proposal system")
    parser.add_argument("cubeid", help="The identifier of the image cube to be accessed e.g. cube-1")
    parser.add_argument("-d", "--destination_directory", help="The directory where the resulting files will be stored",
                        default="cutouts")
    parser.add_argument("-s", "--num_small", help="The number of small cutouts to be produced", default=20, type=int)
    # parser.add_argument("-m", "--num_medium", help="The number of medium cutouts to be produced", default=20, type=int)
    parser.add_argument("-l", "--num_large", help="The number of large cutouts to be produced", default=20, type=int)
    parser.add_argument("--download", help="Download the produced cutouts", action='store_true')

    args = parser.parse_args()
    return args


def get_opal_password(args):
    """
    Retrieve the OPAL password form the user, either form the command line arguments, the file they specified or
      by asking them to input it
    :param args: The parsed command line arguments
    :return: The password
    """
    if args.opal_password:
        return args.opal_password

    if args.passwordfile:
        with open(args.passwordfile, 'r') as fd:
            password = fd.readlines()[0].strip()
    else:
        password = getpass.getpass("Enter your OPAL password: ")

    return password


def get_dimensions(cube_id):
    """ For this test data we will use a predefined dimension object, but this could be read from the TAP service. """

    dims = json.loads(
        '{"axes": [{"name": "RA", "numPixels": "4096", "pixelSize": "5.5555555555560e-04", "pixelUnit": "deg"},' +
        '{"name": "DEC", "numPixels": "4096", "pixelSize": "5.5555555555560e-04", "pixelUnit": "deg"},' +
        '{"name": "STOKES", "numPixels": "1", "pixelSize": "1.0000000000000e+00", "pixelUnit": " ",' +
        '"min": "5.0000000000000e-01", "max": "1.5000000000000e+00", "centre": "1.0000000000000e+00"},' +
        '{"name": "FREQ", "numPixels": "16416", "pixelSize": "1.0000000000000e+00", "pixelUnit": "Hz",' +
        '"min": "1.2699999995000e+09", "max": "1.2700164155000e+09", "centre": "1.2700082075000e+09"}],' +
        '"corners": [{"RA": "1.8942941872444e+02", "DEC": "5.3846168509499e+01"},' +
        '{"RA": "1.8557152279432e+02", "DEC": "5.3846183833748e+01"},' +
        '{"RA": "1.8545899454910e+02", "DEC": "5.6120973603008e+01"},' +
        '{"RA": "1.8954200183991e+02", "DEC": "5.6120957384947e+01"}],' +
        '"centre": {"RA": "1.8750048428742e+02", "DEC": "5.4999722221261e+01"}}')
    return dims


def generate_random_cutouts(args, cube_dim):
    total_cutouts = args.num_small + args.num_large

    # random centres on RA/dec axis - random pixels linearly converted to spatial, so avoid edges
    ra_axis = cube_dim['axes'][0]
    ra_start =  float(cube_dim['corners'][1]['RA'])
    ra_max = int(ra_axis['numPixels']) - _large_cutout_s_len - 10
    ra_vals = np.random.random_integers(10, ra_max, total_cutouts)
    ra_vals = ra_vals * float(ra_axis['pixelSize']) + ra_start
    dec_axis = cube_dim['axes'][1]
    dec_start =  float(cube_dim['corners'][1]['DEC'])
    dec_max = int(dec_axis['numPixels']) - _large_cutout_s_len - 10
    dec_vals = np.random.random_integers(10, dec_max, total_cutouts) * float(dec_axis['pixelSize']) + dec_start

    # loop through producing circle params, use small and large radii
    pos_params = []
    for i in range(0, total_cutouts):
        cutout_radius_degrees = (_large_cutout_s_len if i < args.num_large else _small_cutout_s_len) * float(ra_axis['pixelSize'])
        filter = "CIRCLE " + str(ra_vals[i]) + " " + str(dec_vals[i]) + " " + str(cutout_radius_degrees)
        pos_params.append(filter)

    # random start locations on freq axis - random pixels converted to axis values
    freq_axis = cube_dim['axes'][3]
    freq_max = int(freq_axis['numPixels']) - _large_cutout_em_len - 1
    freq_vals = np.random.random_integers(0, dec_max, 2) * float(freq_axis['pixelSize'])
    band_params = []
    for i in range(0, len(freq_vals)):
        freq_min = freq_vals[i] + float(freq_axis['min'])
        em_len =  _large_cutout_em_len if i == 0 else _small_cutout_em_len
        freq_max = freq_min + (em_len - 1) * float(freq_axis['pixelSize'])
        #print ("FREQ=", float(freq_axis['min']), freq_min, freq_max)
        band_params.append(str(_c/freq_max) + " " + str(_c/freq_min))

    return pos_params, band_params


def main():
    args = parseargs()
    password = get_opal_password(args)

    # Change this to choose which environment to use, prod is the default
    casda.use_at()

    start = time.time()
    if args.destination_directory is not None and not os.path.exists(args.destination_directory):
        os.makedirs(args.destination_directory)

    # Read cube dimensions
    cube_dim = get_dimensions(args.cubeid)
    #print("DIM=", cube_dim)

    # Generate random locations in the cutout
    pos_params, band_params = generate_random_cutouts(args, cube_dim)
    #print("POS=", pos_params)
    #print("BAND=", band_params)

    # Get access to the cube - sia call then datalink
    async_url, authenticated_id_token = casda.get_service_link_and_id(
        args.cubeid,
        args.opal_username,
        password,
        destination_dir=args.destination_directory)
    print (async_url, authenticated_id_token)

    # Create a job to retrieve the cutouts
    job_location = casda.create_async_soda_job([authenticated_id_token], soda_url=async_url)
    casda.add_params_to_async_job(job_location, 'POS', pos_params)
    casda.add_params_to_async_job(job_location, 'BAND', band_params)
    print ('\n\n Job will have %d cutouts.\n\n' % (len(pos_params)*len(band_params)))

    # Run and time the job
    run_start = time.time()
    status = casda.run_async_job(job_location)
    run_end = time.time()
    print('Job finished with status %s in %.02f s\n\n' % (status, run_end - run_start))

    # Optionally download
    print ("Job result available at ", casda.get_results_page(job_location))
    if args.download:
        casda.download_all(job_location, args.destination_directory)

    # Report
    end = time.time()
    print('#### Cutout processing completed at %s ####'
          % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))))
    print('Job was processed in %.02f s' % (run_end - run_start))
    print('Full run took %.02f s' % (end - start))
    return 0


if __name__ == '__main__':
    exit(main())
