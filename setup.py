from distutils.core import setup
from glob import glob
import os
import sys


#html_docs = glob('dbf/html/*')

long_desc="""
Currently supports dBase III, FoxPro, and Visual FoxPro tables. Text is returned as unicode, and codepage settings in tables are honored. Memos and Null fields are supported.  Documentation needs work, but author is very responsive to e-mails.

Not supported: index files (but can create tempory non-file indexes), auto-incrementing fields, and Varchar fields.

Latest release has many backwards incompatibilities, as well as some documentation.  Hopefully the latter will make the former bareable.
"""

if sys.version_info[:2] < (3, 4):
    requirements = ['enum34']
else:
    requirements = []

setup( name='dbf',
       version= '0.96.000',
       license='BSD License',
       description='Pure python package for reading/writing dBase, FoxPro, and Visual FoxPro .dbf files (including memos)',
       long_description=long_desc,
       url='https://pypi.python.org/pypi/dbf',
       packages=['dbf', ],
       provides=['dbf'],
       install_requires=requirements,
       author='Ethan Furman',
       author_email='ethan@stoneleaf.us',
       classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python',
            'Topic :: Database',
            'Programming Language :: Python :: 2.5',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            ],
    )

