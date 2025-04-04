#!/bin/bash


# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install pymssql==2.3.2
pip install -r requirements.txt

# Run the Python script
python3 test_data_generator.py

# Deactivate virtual environment
deactivate
