""" Waybacker retrieval script


"""


import requests
import os
import json
import logging
import datetime
import re
from dateutil import relativedelta
from dateutil import parser as timeparser
import string
from joblib import Parallel, delayed
import argparse

logging.basicConfig(level="CRITICAL")
logger = logging.getLogger(__name__)

WEB_ARCHIVE = "https://web.archive.org/web"
DATADIR     = os.path.join(os.path.dirname(os.path.realpath(__file__)),"data")
CACHEFILE    = os.path.join(DATADIR,'.cache')

def cache_load(url):
    if os.path.exists(CACHEFILE):
        contents = json.load(open(CACHEFILE))
        return contents.get(url,{})
    else:
        return {}

def cache_save(url,status):
    has_from      = status.get('from',False)
    has_to        = status.get('to', False)
    has_current   = type(status.get('current', False))==int
    has_direction = status.get('direction',False)
    if has_from and has_to and has_current and has_direction:
        if os.path.exists(CACHEFILE):
            logger.debug("Updating existing cachefile at {CACHEFILE}".format(CACHEFILE=CACHEFILE))
            cache = json.load(open(CACHEFILE))
        else:
            cache = {}
        cache[url] = status
        json.dump(cache, open(CACHEFILE,'w'),indent=4)
    else:
        logger.critical("Uncorrect status supplied: {status}".format(status=status))
        logger.critical("Missing or incorrect from      : {has_from}".format(**locals()))
        logger.critical("Missing or incorrect to        : {has_to}".format(**locals()))
        logger.critical("Missing or incorrect current   : {has_current}".format(**locals()))
        logger.critical("Missing or incorrect direction : {has_direction}".format(**locals()))

class NonResponse():
    """Mock-Response class for failed attempts

    This class serves to mock responses in case of failed attempts. For example:
    Some urls will yield circular redirects, triggering the 'TooManyRedirects'
    exception. When catching this error, the NonResponse class serves to mock
    the response object as an empty result for this page. 

    """
    status_code = 404
    text        = ''
    encoding    = 'UTF-8'
    headers     = {}
    
    def __init__(self, url,reason):
        """Initialize the mock-response with the url that was sought and 
        the reason for failure
    
        Parameters
        ----
        url : string
            The url attribute of the mock response, most often the page 
            that should have been retrieved.
        reason : string
            The reason for this Mock response, most likely the error 
            or an explanation about why this response does not contain
            any content.
    
        """
        self.url    = url
        self.reason = reason
    

def walk_times(start='now', end='now', step='-2sec'):
    '''Generator for timestamps

    A generator for start-end-step-total_steps tuples which can be used
    for time-slice arguments. 

    Parameters
    ----

    start : string
        A string specifying the timepoint to start collection, either as
        "dd-mm-yyyy", "now" (for current time) or "-2min" For realtive times
    end   : string
        A string specifying the timepoint to end collection, either as
        "dd-mm-yyyy", "now" (for current time) or "-2min" For realtive times

    step : string
        "-2min" For realtive times
    
    Yield
    ----
    tuple
        from_datetime, to_datetime, step, total_steps, direction

    '''

    def parse_time_argument(arg,to_abs=False):
        if type(arg)==datetime.datetime:
            return arg
        elif type(arg)!=str:
            logger.critical("UNKNOWN TIME ARGUMENT '{arg}', should be datetime or string!".format(arg=arg))
        else:
            # now 
            if arg in ['now','NOW']:
                return datetime.datetime.now()

           # DD-MM-YY(YY) dateform
            dateform = re.compile('^(?P<day>[0-9]{1,2})-(?P<month>[0-9]{1,2})-(?P<year>[0-9]{1,4})$')
            
            if dateform.search(arg):
                dates = {k:int(v) for k,v in dateform.search(arg).groupdict().items() if v}
                return datetime.datetime(**dates)
            # Xtime format ( '-2sec')
            xtimeform = re.compile('((?P<seconds>-?\+?[0-9]+)s(ec)?)?((?P<minutes>-?\+?[0-9]+)m[^on](in)?)?((?P<hours>-?\+?[0-9]+)h(our)?)?'
                                     '((?P<days>-?\+?[0-9]+)D(ay)?)?((?P<months>-?\+?[0-9]+)M(on)?)?((?P<years>-?\+?[0-9]+)Y(ear)?)?')
            xtimefound = {k:int(v) for k, v in xtimeform.search(arg).groupdict().items() if v}
            if xtimefound:
                if to_abs:
                    return datetime.datetime.now() + relativedelta.relativedelta(**xtimefound)

                else:
                    return relativedelta.relativedelta(**xtimefound)
            # datetime format
            try:
                result = timeparser.parse(arg)
                logger.debug("turned {arg} into {result}".format(arg=arg, result=result))
                return result

            except ValueError:
                logger.debug("{arg} turns out, not even a isoformatted string".format(arg=arg))

 
        logger.critical("Unkown time specification")

    starttime = parse_time_argument(start, to_abs=True)
    endtime   = parse_time_argument(end  , to_abs=True)
    stepsize  = parse_time_argument(step , to_abs=False)

    stepsecs = stepsize.years*365*24*60*60
    stepsecs += stepsize.months*30*24*60*60
    stepsecs += stepsize.days*24*60*60
    stepsecs += stepsize.hours*60*60
    stepsecs += stepsize.minutes*60
    stepsecs += +stepsize.seconds
    
    print(stepsecs)
    steps = round( abs((starttime-endtime).total_seconds() / stepsecs))
    logger.info("Taking {steps} steps ({stepsize}) between {starttime} and {endtime}".format(
                steps=steps, stepsize=stepsize, starttime=starttime, endtime=endtime))

    if endtime < starttime:
        if not starttime + stepsize < starttime:
            logger.info( "Steps to the past should be negative! (i.e. -2min)")
            stepsize = relativedelta.relativedelta(seconds=-1*stepsecs)
            logger.info("Corrected stepsize to: {stepsize}".format(stepsize=stepsize))
        direction = "downward"
    elif starttime > endtime:
        if starttime + stepsize > starttime:
            logger.info("Steps to the future should be positive! (i.e. 2min)")
            stepsize = relativedelta.relativedelta(seconds=-1*stepsecs)
            logger.info("Corrected stepsize to: {stepsize}".format(stepsize=stepsize))
        direction = 'upward'
    start_datetime = starttime
    end_datetime = endtime

    for step in range(steps):
        yield start_datetime, end_datetime, step, steps, direction
        start_datetime = start_datetime + stepsize

def extract_timestamp(wayback_url):
    """from a wayback archive url, extract the appropriate timestamp as a datetime object

    Parameters
    ----
    wayback_url : string
        The wayback archive URL, formatted according to https://archive.org/about/faqs.php#265.
        for example "https://web.archive.org/web/20150420000044/http://online.wsj.com/", where
        the notation is "https://web.archive.org/web/<YYYY><mm><dd><HH><MM><SS>/<target_url>".

    Returns
    ----
    datetime object

    """
    logger.debug("Extracting timstamp from {wayback_url}".format(wayback_url=wayback_url))
    no_wayback        = wayback_url[len(WEB_ARCHIVE)+1:]
    timestring, rest  = no_wayback.split('/',1)
    datetime_format   = "%Y%m%d%H%M%S"
    timestamp         = datetime.datetime.strptime(timestring, datetime_format)
    logger.debug("Timestamp is {timestamp}".format(timestamp=timestamp))
    return timestamp

def get_page(url, timestamp, **kwargs):
    '''Retrieve a page from the Wayback Archive for a specific timestamp

    Parameters
    ----
    url : string
        The address of the page to retrieve, e.g. https://www.nytimes.com/
    timestamp : datetime
        A datetime object indicating the preferred wayback time to fetch. The
        Wayback Archive automatically grabs the closest available date, see
        https://archive.org/about/faqs.php#265 
    kwargs : keyword arguments
        A way to pass additional information about the pages. 

    Returns
    ----
    dictionary
        target_url             : the URL argument
        target_timestamp       : the ISO-formatted timestamp argument
        defacto_url            : the actual Wayback archive URL obtained
        defacto_timestamp      : the defacto timestamp of the page obtained inferred from the wayback URL
        sec_relative_to_target : the relative (target - obtained) seconds between the target and defacto timestamp
        status_code            : the status code of the obtained response (automatic redirects are not visible)
        reason                 : HTTP reason for status code
        text                   : response text content
        encoding               : the requests inferred encoding of the text
        response_headers       : the requests based response headers
        sec_elapsed            : the seconds between the request and the response to the wayback archive
        **kwargs
        
    '''
    # Format target URL
    target_timestamp = "{year}{month:02d}{day:02d}{hour:02d}{minute:02d}{second:02d}".format(
        year=timestamp.year, month=timestamp.month, day=timestamp.day,
        hour = timestamp.hour, minute=timestamp.minute, second=timestamp.second)
    target_url = "{wayback_url}/{target_timestamp}/{url}".format(
            wayback_url=WEB_ARCHIVE, target_timestamp=target_timestamp, url=url)

    # Retrieving page
    logger.debug("Retrieving from {target_url}".format(target_url=target_url))
    start_of_capture = datetime.datetime.now()
    try:
        response = requests.get(target_url)
    except requests.exceptions.TooManyRedirects:
        response = NonResponse(url=target_url, reason="Too many redirects")   
        
    if response.status_code == 200:
        logger.debug("Succesfully retrieved {target_url}".format(target_url=target_url))
    else:
        logger.debug("Status code {response.status_code} for {target_url} because of {response.reason}".format(
            response=response, target_url=target_url))
    end_of_capture = datetime.datetime.now()
    # format results
    retrieved_timestamp = extract_timestamp(response.url)
    time_delta          = timestamp - retrieved_timestamp
    page_dict = {
        'target_url'            : target_url,
        'target_timestamp'      : timestamp.isoformat(),
        'defacto_url'           : response.url,
        'defacto_timestamp'     : retrieved_timestamp.isoformat(),
        'sec_relative_to_target': time_delta.total_seconds(),
        'status_code'           : response.status_code,
        'reason'                : response.reason,
        'text'                  : response.text,
        'encoding'              : response.encoding,
        'respons_headers'       : dict(response.headers),
        'sec_elapsed'           : (end_of_capture-start_of_capture).total_seconds(),
        'retrieved_at'          : end_of_capture.isoformat()
        
    }
    page_dict.update(**kwargs)
    return page_dict

def clean_filename(url):
    """Make a URL into an posix friendly filename

    Parameters
    ----
    url : string
        string that contains illegal characters, e.g. "http://online.wsj.com/"

    Returns
    ----
    string
        Filename filtered to include only ascii_letters and digits + '-_() '
        periods ('.') and colons (':') are replaced with underscores ('_'),
        e.g.: "https_www_wsj_com"

    """
    valid_chars = "-_() %s%s" % (string.ascii_letters, string.digits)
    nodot       = url.replace('.','_').replace(':','_')
    filename = ''.join([c for c in nodot if c in valid_chars])
    return filename



def main(url, from_time, to_time, stepsize, reset, debug, silent, batchsize = 10, threads=-1, outputdir=None):
    """The main eventloop for wayback archive retrieval

    Process for retrieval:
    1) override global output directory and cachefile location if required (set by outputdir)
    2) set appropriate logging levels depending on debug and silent flags
    3) For given url, create posix filename and ensure the output directory exists, reset if reset==True
    4) Check if a Cachefile exists and contains an entry for this URL, resume if true
    5) Open file and start parallel retrieval with batch write to disk

    Parameters
    ----
    url : string
        The original URL to retrieve from the wayback archive, e.g. "http://online.wsj.com/"
    from_time : string
        time to start, expressed as 'now', '01-01-2018', '-2D' formats
    to_time   : string
        time to stop, expressed as 'now', '01-01-2018', '-2D' formats
    stepsize  : string
        stepsize to take between pages, takes the '(-)X' format, where negative
        numbers express steps backwards in time. X can be the increment size:
        s : seconds, 
        m : minutes, 
        h : hours, 
        D : day, 
        M : Month, 
        Y : Year
        Example : '-2D', for get page with 48 hour intervals from wayback archive
    reset : bool
        Whether to clear cache AND data and start over
    debug : bool
        Whether to log debug statements
    silent : bool
        Whether to log INFO statements
    batchsize : int (default=10)
        The number of responses to write to disk together
    threads : int (default=-1)
        The number of parallel threads to use for retrieval, defaults to N-cores available -1
    outputdir : string (default='data')
        The directory to write output AND cachefile to, defaults to "data"

    Returns
    ----
    None

    Notes:
        Function writes data to disk

    """
    # Override output directory if required
    global DATADIR
    global CACHEFILE
    if outputdir and outputdir != DATADIR:
        logger.info("Changing output directory to {outputdir}".format(outputdir=outputdir))
        DATADIR = outputdir
        CACHEFILE = os.path.join(DATADIR,'.cache')
        logger.debug("Set output to {DATADIR}".format(DATADIR=DATADIR))

    # Set appropriate logging levels
    if debug:
        logger.setLevel("DEBUG")
        logger.debug("Debugmode ENGAGED")
    elif not silent:
        logger.setLevel("INFO")

    # Prepare output location
    target_file = clean_filename(url)

    os.makedirs(DATADIR, exist_ok=True)

    if target_file in os.listdir(DATADIR) and reset:
        logger.info("Resetting file {filename}".format(filename=os.path.join(DATADIR,target_file)))
        os.remove(os.path.join(DATADIR,target_file))
    if os.path.exists(CACHEFILE) and reset:
        logger.debug("Resetting cache {CACHEFILE}".format(CACHEFILE=CACHEFILE))
        os.remove(CACHEFILE)
    
    # Check resume state
    status = cache_load(url)
    if status:
        logger.info("Resuming previous collection:\n {status}".format(status=status))
        from_time = status['from']
        to_time   = status['to']
        stepsize  = status['stepsize']
        current   = status['current']
    else:
        current = 0
        status['from'     ] = from_time
        status['to'       ] = to_time
        status['stepsize' ] = stepsize
        status['current'  ] = current
        status['direction'] = 'unknown'

    # Do data collection 
    with open(os.path.join(DATADIR,target_file), 'a+') as f:
        batch = []
        for start, _,  step, total, direction in walk_times(from_time, to_time, stepsize):
            if status['from'] == 'now':
                status['from'] = start.isoformat()
            status['direction'] = direction
            if not step%10: logger.debug("now at {step} of {total}".format(step=step, total=total))
            if step < current:
                continue    
            batch.append({'url':url, 'timestamp':start, 'step':step})
            if len(batch)==batchsize:
                perc=(step/total)*100
                logger.info("Processing {batchsize} pages for {url} at step {step:6.0f} of {total:6.0f} {perc:3.2f}%".format(
                batchsize=batchsize, url=url, step=step, total=total, perc=perc))
                retrieved = Parallel(threads)(delayed(get_page)(**args) for args in batch)
                for hit in retrieved:
                    f.write(json.dumps(hit)+"\n")
                    status['current'] = hit['step']
                logger.info("Wrote batch to disk")
                cache_save(url,status)
                batch=[]
        retrieved = Parallel(threads)(delayed(get_page)(**args) for args in batch)
        for hit in retrieved:
            f.write(json.dumps(hit)+"\n")
            status['current'] = hit['step']
            cache_save(url,status)
        logger.info("wrote last batch to disk")
        batch=[]   
    logger.info("Succesfully stopped retrieval")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-f", "--from", help="time to start, expressed as 'now', '01-01-2018', '-2D' formats", 
                        dest='fromtime', default="now")
    parser.add_argument("-t", "--to", help="time to stop, expressed as 'now', '01-01-2018', '-2D' formats",
                        dest="totime", default="-7D")
    parser.add_argument("-s", "--step", help="stepsize to take between pages, takes the '(-)X' format, where negative "
                                             "numbers express steps backwards in time. X can be the increment size:\n"
                                             "s : seconds, m : minutes, h : hours, D : day, M : Month, Y : Year ",
                        default="-1D")
    parser.add_argument('-b','--batchsize', help="the number of results to bundle, (higher means bigger times between "
                                                 "writing to disk, but also lower overhead)",
                        type=int, default=10)
                        
    parser.add_argument("-d", "--debug", help="print debug statements", default=False, action="store_true")
    parser.add_argument("-r", "--reset", help="remove prior results and start over", default=False, action="store_true")
    parser.add_argument("-q", "--quiet",help="do not print progress to stdout", default=False, action='store_true')
    parser.add_argument("-p", "--parallel", help="number of parallel_threads to use", type=int, default=-1)
    parser.add_argument("url", help="the URL to obtain from the wayback archive")
    parser.add_argument("-o","--output-dir", help="Directory to store results", default=DATADIR)

    args = parser.parse_args()
    
    
    main(url=args.url, from_time=args.fromtime, to_time=args.totime, stepsize=args.step, reset=args.reset, debug=args.debug, 
         silent=args.quiet, threads=args.parallel, outputdir=args.output_dir)
    
    
    
