#!/bin/bash

# may need to install/ update Cython 
# python3 -m pip install -U Cython

if
    # for arm architecure, need to set BLIS to generic for install to work
    lscpu| head -n 1 | grep -q 'aarch\|arm'; then
    echo "Setting BLIS_ARCH to generic"
    export BLIS_ARCH='generic'
fi
echo "Installing Chatterbot"
python3 -m pip install .

# change 'en' to any other language if desired; by default the small model is downloaded
# but this can be changed; for example: change 'en' to 'en_core_web_lg' for the large model
# then will need to create corresponding shortcut link; for example: python3 -m spacy link --force en_core_web_lg en
echo "Downloading and linking spaCy en model"
python3 -m spacy download en
