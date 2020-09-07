
#!/bin/bash
cd /Users/rraymond/PycharmProjects/moviedb
# check virtual env is installed at user level
deactivate 2> /dev/null
pip3 show virtualenv
if [ $? -ne 0 ] ; then
   pip3 install --upgrade pip
   pip3 install --upgrade setuptools
   pip3 install virtualenv
fi

# now lets build venv
python3 -m venv venv
source venv/bin/activate
if [ $? -ne 0 ] ; then
   pwd
   exit 1
fi

# install additional dev tools
pip3 install --upgrade pip
pip3 install --upgrade setuptools


# install the app libararies
echo "======= app stuff ========"
pip3 install -r requirements.txt    
