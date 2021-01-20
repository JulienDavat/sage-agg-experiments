#!/bin/bash

# creates a virtual environement to isolate project dependencies
virtualenv aggregates

# activates the virtual environement
source aggregates/bin/activate

# installs main dependencies
npm install
pip install -r requirements.txt

# installs the SaGe client
cd client/sage
gradle fatJar
cd ../..

# installs the SaGe server
cd server/sage-agg
pip install -r requirements.txt
pip install pybind11
pip install -e .[hdt,postgres]
cd ../..