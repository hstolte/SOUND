#!/usr/bin/env bash
set -e
echo "Downloading datasets..."
gdown -O data/input/datasets.zip https://drive.google.com/u/0/uc?id=1uNOUlCoa9CfH7WCxe3nSsCwQVVjf9teB
echo
echo "Extracting files..."
echo
unzip -q data/input/datasets.zip -d data/input/
rm data/input/datasets.zip
echo
echo "Download successful!"
echo
