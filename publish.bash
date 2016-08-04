#!/bin/bash --login

# This script ensures that the version numbers from cocnord/constants.py
# and dcos/setup.py are in sync. If the --no-bump flag isn't passed in
# then the tiny number will be incremented automatically before publishing

set -e
cur=$(pwd)
git_root=$(git rev-parse --show-toplevel)

cd $git_root

# Ensure versions are the same, parse for current version
version=$(grep 'version' concord/constants.py | awk '{print $3}' | tr -d "'")
maj_minor=$(echo $version | grep -Po '^\d+\.\d+\.')
tiny=$(echo $version | grep -Po '(\d+)$')

# Increment tiny unless flag is passed
if [[ $* != *--no-bump* ]]; then
    tiny=$((tiny+1))
fi
new_version="$maj_minor$tiny"

# Take new version and insert line in respective files
sed -i "s/^version = '${version}'$/version = '${new_version}'/" concord/constants.py
sed -i "s/^dcos_concord_version.*$/dcos_concord_version = '${new_version}'/" dcos/setup.py

echo "Current version:" $version
echo "New version:" $new_version

# Publish concord cli
python setup.py register sdist upload

# Publish dcos concord cli
cd $git_root/dcos
python setup.py register sdist upload

cd $cur

