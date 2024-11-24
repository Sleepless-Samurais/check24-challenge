#!/bin/bash

# Run the Python script
python3 init.py
gunicorn main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000 --workers 4 --threads 16