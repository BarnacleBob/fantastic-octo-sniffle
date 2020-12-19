import requests
from requests.auth import HTTPBasicAuth
from urllib import parse
from http.client import HTTPConnection

from absl import app
from absl import flags
from absl import logging

import logging as oglogging

import pdb
import json
import math

from python_graphql_client import GraphqlClient
from datetime import datetime

FLAGS = flags.FLAGS

flags.DEFINE_string('client_id', None, 'Client id')
flags.mark_flag_as_required('client_id')
flags.DEFINE_string('client_secret', None, 'Client secret')
flags.mark_flag_as_required('client_secret')
flags.DEFINE_enum('log_level', 'INFO', ['FATAL', 'ERROR','WARNING','INFO', 'DEBUG'], 'log level')
flags.DEFINE_boolean('requests_debug', False, 'debug http requests')
flags.DEFINE_integer('parse_percentile', 75, 'parse percentile to use')

# 92470b99-fb1d-476b-a0ab-07318b6112e1 and the client secret is 2ijsitVGVpvPXPXaEW1R1PvMZikVT9SwWec0iyVy
# 92470b99-fb1d-476b-a0ab-07318b6112e1:2ijsitVGVpvPXPXaEW1R1PvMZikVT9SwWec0iyVy
# eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5MjQ3MGI5OS1mYjFkLTQ3NmItYTBhYi0wNzMxOGI2MTEyZTEiLCJqdGkiOiI0MGJkYmE1ZTk5NGQ2ZDU1NmNkNmIxYjU4YmI1M2FlYzgwNzliY2I2ZjgzMWRiNWY3NDc2YTM5NmZiMWJkZDE4ZjdkYjZjMGExMzAyMmVjNiIsImlhdCI6MTYwODMzODQyMSwibmJmIjoxNjA4MzM4NDIxLCJleHAiOjE2MTA5MzA0MjEsInN1YiI6IiIsInNjb3BlcyI6WyJ2aWV3LXByaXZhdGUtcmVwb3J0cyJdfQ.YjyYXddwz7mPsHaFBRnhGWTx8KWT8AusmlQ-RVArBwxgKtBax3ohlQqwBcU7PN31ktjgnTCzu8wzSzzMGov1HmZy0qS38X66sYBxaNnQNAkN0O6F7OXMcoMhoCihtQKFnMD-_85DwLnXnVUYwgQSJqHTyegU21w--7awdjuAfmavYT2LKLYGNYwP27sopCfUrtnjFwd0LHByRbh64rJ6uc89qqyIPjzXfWXbJfIvjJoVIxTyxS5-sbdLhyBobgZPuJZPjaY6G5FNt750Se_O1dvuDonqypsPHZ0C8GNx58hXN1n3Rpp34STSxrkcL3GQLbEOPv4QUj4_U47mHmnpohEpp6na2yiBRSL0AQRDC86Wirj_UefW1VQrf5qybNE6Okil2xl5F-A-HRcTuTPNPEs5J1LP_-5KIVQ8fU-JuvZNvxIm6V9xu9xh3eos3AuY_WTqjW_ynUAzhMRzPPmyctuJaIf1dXRFhLwqW_oZAhHEgnwCouxxJV5owTv0pT976nbDNpvocRnjInsK6sBD5DA-EKY7rLAolt1h593azWVv_boSRCRrw-CFy49nSV0KAC9KyWf4K5c2MGIUruPZ0BZ6LgFni9rH5O81i-IJsFjI3ZWqv9NOIzp6HT_FWw4V9EX69ykHkrQnS_s9tyQvVZGT7DNCht8CnvJuUKEuzyis

# curl -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5MjQ3MGI5OS1mYjFkLTQ3NmItYTBhYi0wNzMxOGI2MTEyZTEiLCJqdGkiOiI0MGJkYmE1ZTk5NGQ2ZDU1NmNkNmIxYjU4YmI1M2FlYzgwNzliY2I2ZjgzMWRiNWY3NDc2YTM5NmZiMWJkZDE4ZjdkYjZjMGExMzAyMmVjNiIsImlhdCI6MTYwODMzODQyMSwibmJmIjoxNjA4MzM4NDIxLCJleHAiOjE2MTA5MzA0MjEsInN1YiI6IiIsInNjb3BlcyI6WyJ2aWV3LXByaXZhdGUtcmVwb3J0cyJdfQ.YjyYXddwz7mPsHaFBRnhGWTx8KWT8AusmlQ-RVArBwxgKtBax3ohlQqwBcU7PN31ktjgnTCzu8wzSzzMGov1HmZy0qS38X66sYBxaNnQNAkN0O6F7OXMcoMhoCihtQKFnMD-_85DwLnXnVUYwgQSJqHTyegU21w--7awdjuAfmavYT2LKLYGNYwP27sopCfUrtnjFwd0LHByRbh64rJ6uc89qqyIPjzXfWXbJfIvjJoVIxTyxS5-sbdLhyBobgZPuJZPjaY6G5FNt750Se_O1dvuDonqypsPHZ0C8GNx58hXN1n3Rpp34STSxrkcL3GQLbEOPv4QUj4_U47mHmnpohEpp6na2yiBRSL0AQRDC86Wirj_UefW1VQrf5qybNE6Okil2xl5F-A-HRcTuTPNPEs5J1LP_-5KIVQ8fU-JuvZNvxIm6V9xu9xh3eos3AuY_WTqjW_ynUAzhMRzPPmyctuJaIf1dXRFhLwqW_oZAhHEgnwCouxxJV5owTv0pT976nbDNpvocRnjInsK6sBD5DA-EKY7rLAolt1h593azWVv_boSRCRrw-CFy49nSV0KAC9KyWf4K5c2MGIUruPZ0BZ6LgFni9rH5O81i-IJsFjI3ZWqv9NOIzp6HT_FWw4V9EX69ykHkrQnS_9tyQvVZGT7DNCht8CnvJuUKEuzyis" https://www.warcraftlogs.com/api/v2/client

class WarcraftLogsClient:
    def __init__(self):
        self._authorize()
        self._wcl = GraphqlClient(
            endpoint='https://www.warcraftlogs.com/api/v2/client',
            headers={'Authorization': 'Bearer %s' % self.access_token},
        )

    def _authorize(self):
        params={'grant_type': 'client_credentials'}
        logging.debug('authorizing with %s:%s', FLAGS.client_id, FLAGS.client_secret)
        auth=HTTPBasicAuth(FLAGS.client_id, FLAGS.client_secret)
        logging.debug('Attempting to login')
        r = requests.post('https://www.warcraftlogs.com/oauth/token', data=params, auth=auth)
        logging.debug(r)
        response=json.loads(r.content)
        logging.debug(response)
        self.access_token = response['access_token']
        logging.debug('using access token: %s', self.access_token)

    def _query(self, query, variables):
        logging.debug('querying: %s', query)
        logging.debug('with vars: %s', variables)
        response = self._wcl.execute(query=query, variables=variables)
        logging.debug('response: %s', response.__str__()[0:2000])
        return response

    def getGuildId(self, name, server, region):
        query = """
            query ($name: String, $server: String, $region: String) {
                guildData{
                    guild(name: $name, serverSlug: $server, serverRegion: $region){
                        id
                    }
                }
            }
        """
        vars={'name': name, 'server': server, 'region': region}
        return self._query(query, vars)['data']['guildData']['guild']['id']

    def getReports(self, guildId, limit=50):
        query = """
            query getReports($guildId: Int, $limit: Int) {
              reportData{
                reports(guildID: $guildId, limit: $limit){
                  data{
                    code
                    startTime
                    rankings
                  }
                  total
                }
              }
            }
        """
        vars = {'guildId': guildId, 'limit': limit}
        return self._query(query, vars)['data']['reportData']['reports']['data']

    def explodeReports(self, reportData):
        exploded=[]
        for report in reportData:
            logging.debug('processing report: %s', report.__str__()[0:2000])
            logging.debug('report is from %s', datetime.utcfromtimestamp(report['startTime']/1000).strftime('%Y-%m-%d %H:%M:%S'))
            for fight in report['rankings']['data']:
                logging.debug('processing fight: %s', fight.__str__()[0:2000])
                for role in fight['roles']:
                    logging.debug('processing role %s', role)
                    for character in fight['roles'][role]['characters']:
                        logging.debug('processing character: %s', character.__str__()[0:2000])
                        record = {}
                        record['fightId'] =  '%s-%s' % (report['code'], fight['fightID'])
                        record['boss'] = fight['encounter']['name']
                        record['kill'] = fight['kill']
                        record['role'] = role
                        for attr in ['id','name','bracketPercent', 'rankPercent']:
                            record[attr] = character[attr]
                        exploded.append(record)
        logging.debug('exploded: %s', exploded.__str__()[0:2000])
        return exploded

def main(argv):
    del argv  # Unused.
    logging.set_verbosity(getattr(logging, FLAGS.log_level))
    if FLAGS.requests_debug:
        HTTPConnection.debuglevel = 1
        oglogging.basicConfig()
        oglogging.getLogger().setLevel(logging.DEBUG)
        requests_log = oglogging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(getattr(logging, FLAGS.log_level))
        requests_log.propagate = True
    logging.info('Starting up')
    wcl=WarcraftLogsClient()
    logging.info('Finding guild')
    guildId = wcl.getGuildId('Legal Tender', 'Lightbringer', 'US')
    logging.info('Found %s', guildId)
    reportData = wcl.getReports(guildId, 5)
    records = wcl.explodeReports(reportData)
    fightIds = []
    attendance = {}
    parse = {}
    ilvlparse = {}
    for record in records:
        if record['fightId'] not in fightIds:
            fightIds.append(record['fightId'])
        if record['name'] not in attendance:
            attendance[record['name']] = []
        attendance[record['name']].append(record['fightId'])

        if record['name'] not in parse:
            parse[record['name']] = []
        parse[record['name']].append(record['rankPercent'])

        if record['name'] not in ilvlparse:
            ilvlparse[record['name']] = []
        ilvlparse[record['name']].append(record['bracketPercent'])

    logging.debug('fight ids: %s', fightIds)
    logging.debug('attendance: %s', attendance)

    attendance_score = {}
    for character in attendance:
        attendance_score[character] = len(attendance[character]) / len(fightIds) * 100.0

    logging.info('attendance score: %s', attendance_score)

    parse_score = {}
    for character in parse:
        percentile_index = math.ceil(len(parse[character]) * (FLAGS.parse_percentile / 100.0))
        logging.debug('parse count: %s, percentile index: %s', len(parse[character]), percentile_index)
        parse[character].sort()
        parse_score[character] = parse[character][percentile_index - 1]

    logging.info('percentile score: %s', parse_score)

    ilvlparse_score = {}
    for character in ilvlparse:
        percentile_index = math.ceil(len(ilvlparse[character]) * (FLAGS.parse_percentile / 100.0))
        ilvlparse[character].sort()
        ilvlparse_score[character] = ilvlparse[character][percentile_index - 1]

    logging.info('ilvl percentile score: %s', ilvlparse_score)

if __name__ == '__main__':
  app.run(main)
