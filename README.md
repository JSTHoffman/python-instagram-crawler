# Instagram Crawler

Crawl public Instagram profiles to collect post data.


# Installation

If you don't use `pipsi`, you're missing out.
Here are [installation instructions](https://github.com/mitsuhiko/pipsi#readme).

Clone this repository and run:

    $ cd /path/to/repository
    $ pipsi install .


# Usage

To use it:

    $ instagram-crawler --help

You will be prompted to enter the path to a .CSV file with a column containing
usernames for public Instagram profiles, start and end dates, and a name for
the output file. The output will be saved in .CSV format to:

`/repository/path/output/<file_name>.csv`
