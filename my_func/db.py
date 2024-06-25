import sys
import os

sys.path.append('/Users/hj/Dropbox/real_estate/model/my_func')
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
import api
import pandas as pd
import matplotlib.pylab as plt
from sklearn.linear_model import LinearRegression
import numpy as np
import seaborn as sns
from matplotlib import rc
import time
import preprocess as pre

pre.execute_query('DROP TABLE test IF EXISTS')

