#!/bin/bash

# Run all tests if no arguments are provided
if [ $# -eq 0 ]; then
    python -m unittest discover -s tests
else
    # Run specific test class or test method
    python -m unittest "$@"
fi