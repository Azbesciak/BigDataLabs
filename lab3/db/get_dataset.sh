#!/usr/bin/env bash

# Download Million Song Dataset
if [ ! -e unique_tracks.zip ] && [ ! -e unique_tracks.txt ]; then
    wget http://www.cs.put.poznan.pl/kdembczynski/lectures/data/unique_tracks.zip
fi

if [ ! -e triplets_sample_20p.zip ] && [ ! -e triplets_sample_20p.txt ]; then
    wget http://www.cs.put.poznan.pl/kdembczynski/lectures/data/triplets_sample_20p.zip
fi

# Unzip files
if [ -e unique_tracks.zip ] && [ ! -e unique_tracks.txt ]; then
    unzip unique_tracks.zip
fi

if [ -e triplets_sample_20p.zip ] && [ ! -e triplets_sample_20p.txt ]; then
    unzip triplets_sample_20p.zip
fi

cp unique_tracks.txt sqlite_example/unique_tracks.txt
cp triplets_sample_20p.txt sqlite_example/triplets_sample_20p.txt

cp unique_tracks.txt sqlite_fast/unique_tracks.txt
cp triplets_sample_20p.txt sqlite_fast/triplets_sample_20p.txt
