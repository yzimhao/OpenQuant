
from pip.req import parse_requirements

from setuptools import (
    find_packages,
    setup,
)

setup(
    name='rqalpha_mod_futu',
    version='0.1.0',
    description='futu api for RQAlpha',
    packages=find_packages(exclude=[]),
    author='futu',
    author_email='futu email',
    license='Apache License v2',
    package_data={'': ['*.*']},
    url='https://github.com/FutunnOpen/OpenQuant',
    install_requires=[str(ir.req) for ir in parse_requirements("requirements.txt", session=False)],
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)