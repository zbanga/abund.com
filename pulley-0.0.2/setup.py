import sys
from setuptools import setup, find_packages

setup(
    name='pulley',
    version='0.0.2',
    author='Jimmie Goode',
    author_email='jimmie@goodeanalytics.com',
    url='https://github.com/jgoode21/pulley.git',
    description='Live trading with zipline and swigibpy via Interactive Brokers.',
    long_description='',
    packages=find_packages(),
    license='',
    classifiers=[
    ],
    install_requires=[
        'numpy',
        'pandas',
         #'zipline', # requires a custom zipline that should be installed first
        'swigibpy',
        'psycopg2',
        'tzlocal',
        'django',
        'celery',
        'django-celery',
    ],
)
