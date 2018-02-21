# Waybacker

## Purpose

The waybacker is build as a commandline script to retrieve pages from the wayback archive. Typically, the user is interested
in the contents of a page over time and wants to get this page every N-time between two dates. This script is build to support
such behaviour. 

## Usage

The waybacker script is set up for pretty quick & easy use (although untested outside of linux!). 

```bash
python waybacker.py -f "-7D" -t "now" -s "1D" example.com

```

The output of the waybacker script can be found in the `data` folder, with a filename corresponding to the given URL. 
Read the files (in python) by using:

```python

with open(filename) as f: 
    results = []
    line = f.readline():
    while line:
        results.append(json.loads(line))
        line = f.readline()

```

You can also find the help: 

```bash

python waybacker.py -h

usage: waybacker.py [-h] [-f FROMTIME] [-t TOTIME] [-s STEP] [-d] [-r] [-q]
                    [-p PARALLEL]
                    url

positional arguments:
  url                   the URL to obtain from the wayback archive

optional arguments:
  -h, --help            show this help message and exit
  -f FROMTIME, --from FROMTIME
                        time to start, expressed as 'now', '01-01-2018', '-2D'
                        formats
  -t TOTIME, --to TOTIME
                        time to stop, expressed as 'now', '01-01-2018', '-2D'
                        formats
  -s STEP, --step STEP  stepsize to take between pages, takes the '(-)X'
                        format, where negative numbers express steps backwards
                        in time. X can be the increment size: s : seconds, m :
                        minutes, h : hours, D : day, M : Month, Y : Year
  -d, --debug           print debug statements
  -r, --reset           remove prior results and start over
  -q, --quiet           do not print progress to stdout
  -p PARALLEL, --parallel PARALLEL
                        number of parallel_threads to use


```
