import datetime
from gzip import decompress
from time import sleep
from io import StringIO
import logging
import pandas as pd
import pysftp

logger = logging.getLogger("bberg.sftp")


def send_request(sftp, rid, request):
    """
    Submits a request to the sftp server
    :param sftp: A connection to the Bloomberg (s)ftp server directory.
    :param rid: The request id for this request.
    :param request: A formatted request for bloomberg.
    """
    with sftp.open('{}.req'.format(rid), mode='w') as request_file:
        request_file.write(request)
        logger.info('Submitted request id: {} to Bloomberg sftp server'.format(rid))


def parse_hist_security_response(response, begin_date, end_date, fields):
    in_sec = False
    dframes = {}
    for line in StringIO(response):
        bits = line.split('|', 4)
        if in_sec:
            if bits[0] == 'END SECURITY':
                in_sec = False
                ret_code = int(bits[3])
                if ret_code != 0:
                    estr = "Received return code: {} for history request for security: {}"
                    logger.warn(estr.format(ret_code, sec))
            else:
                assert bits[0] == sec
                try:
                    val = float(bits[2])
                except ValueError:
                    val = None
                    logger.debug("Could not convert val: {} to float".format(bits[2]))
                if val:
                    dframe.loc[datetime.datetime.strptime(bits[1], '%d/%m/%Y'), col] = val

        elif bits[0] == 'START SECURITY':
            in_sec = True
            sec = bits[1]
            col = bits[2]
            if sec in dframes:
                dframe = dframes[sec]
            else:
                dframe = pd.DataFrame(index=pd.date_range(begin_date, end_date), columns=fields)
                dframes[sec] = dframe

    return dframes


def get_response(sftp, rid, responses):
    """
    Populates dictionary responses with the response from this request id if it exists.
    :param sftp: An sftp connection to Bloomberg
    :param rid: The request id
    :param responses: The dictionary to populate the response with.
    :return: True if response available and processed, false otherwise.
    """
    replyname = '{}.dat.gz'.format(rid)
    if sftp.exists(replyname):
        with sftp.open(replyname) as replyfile:
            responses[rid] = decompress(replyfile.read()).decode("utf-8")
        return True
    # Response not ready so return false.
    return False


class Sftp(object):
    _opening_stanza = 'START-OF-FILE\n'
    _closing_stanza = '\nEND-OF-FILE\n'
    _start_of_fields = '\nSTART-OF-FIELDS\n'
    _end_of_fields = '\nEND-OF-FIELDS\n'
    _start_of_data = 'START-OF-DATA\n'
    _end_of_data = '\nEND-OF-DATA\n'

    def __init__(self, host, username, password):
        self._tstr = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self._request_id = 0
        self._host = host
        self._username = username
        self._password = password

    def _build_header(self, headers):
        self._request_id += 1
        rid = 'bbpy_{}_{}'.format(self._tstr, self._request_id)
        header = '''
            FIRMNAME={}
            REPLYFILENAME={}.dat
            COMPRESS=yes
            PROGRAMFLAG=oneshot
        '''.format(self._username, rid)

        return rid, header + '\n'.join('{}={}'.format(key, val) for key, val in headers.items())

    def _build_fields(self, fields):
        return self._start_of_fields + '\n'.join(fields) + self._end_of_fields

    def _build_data(self, data):
        return self._start_of_data + '\n'.join(data) + self._end_of_data

    def build_request(self, headers, fields, data):
        """
        Createsd a request suitable for use with request().
        :param headers: A dict with the header options to set
        :param fields: A list of the fields we want
        :param data: A list of the lines for the data section.
        :return: (rid, request)
                    - rid is the identifier of the request
                    - request is the request text.
        """
        rid, hdrs = self._build_header(headers)
        return rid, ''.join([self._opening_stanza,
                             hdrs,
                             self._build_fields(fields),
                             self._build_data(data),
                             self._closing_stanza])

    def request(self, requests):
        """
        Make a synchronous request using the sftp service. Waits until a response is available for all requests.
        :param requests: A dict from request id to the request text for all the requests we want to process.
        :return: A dict from request id to response contents (A large multi-line string)
        """
        # TODO: Handle failure cases
        responses = {}
        with pysftp.Connection(self._host, username=self._username, password=self._password) as sftp:
            # Send each request in the collection to Bloomberg, and wait until they are all done.
            pending = []
            for rid, request in requests.items():
                send_request(sftp, rid, request)
                pending.append(rid)

            while len(pending) > 0:
                sleep(30)
                pending = [rid for rid in pending if not get_response(sftp, rid, responses)]
                assert len(pending) + len(responses) == len(requests)

        return responses
