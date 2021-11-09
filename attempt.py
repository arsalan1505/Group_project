import boto3
import csv
import pandas as pd
import numpy as np
import os
import psycopg2
import psycopg2.extras
import sys
from itertools import chain

import handler as hnd