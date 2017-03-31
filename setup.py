"""
Crawl public Instagram profiles to collect post data.
"""
from setuptools import find_packages, setup
import os

dependencies = ['click', 'selenium', 'bs4', 'pandas', 'requests']

# MAKE OUTPUT DIRECTORY IF NONE EXISTS
if not os.path.isdir('./output'):
    os.makedirs('./output')

setup(
    name='python-instagram-crawler',
    version='0.1.0',
    url='https://github.com/jsthoffman/python-instagram-crawler',
    license='BSD',
    author='Jaime Hoffman',
    author_email='jsthoffman90@gmail.com',
    description='Crawl public Instagram profiles to collect post data.',
    long_description=__doc__,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=dependencies,
    entry_points={
        'console_scripts': [
            'instagram-crawler = instagram_crawler.cli:main',
        ],
    },
    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        # 'Development Status :: 1 - Planning',
        # 'Development Status :: 2 - Pre-Alpha',
        # 'Development Status :: 3 - Alpha',
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
