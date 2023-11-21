#!/bin/env python

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--channels', type=int, nargs='*', default=None)
parser.add_argument('-f', '--filenames', type=str, nargs='*', default=None)
args = parser.parse_args()

if args.channels is None and args.filenames is None:
    parser.error('No input provided.')

if args.filenames:
    channels = []
    for filename in args.filenames:
        chan = int(filename.split('_ch')[1].split('_')[0])
        channels.append(chan)
else:
    channels = args.channels

bands = []
temp_list = []
for idx in range(len(channels)):
    if idx == 0 or abs(channels[idx] - channels[idx-1]) == 1:
        temp_list.append(channels[idx])
        if idx == len(channels) - 1:
            bands.append(temp_list)
    else:
        bands.append(temp_list)
        temp_list = [channels[idx]]

for band in bands:
    low_chan = min(band)
    high_chan = max(band)
    with open(f'channels_{low_chan:03d}_{high_chan:03d}_indices.txt', 'w') as f:
        for chan in band:
            f.write(f'{chan:03d}\n')