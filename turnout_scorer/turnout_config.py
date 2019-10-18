""" Config class for the turnout score calculator. It checks the correctness of the config
    and creates the class based on user input to be used in turnout scorer """

import datetime
import logging

REQUIRED_FIELDS_IN_CONFIG = {'voter_file': ['bucket', 'key_pattern'],
                             'column_names': ['dob_field', 'id_field', 'party_field',
                                              'reg_date_field'],
                             'formats': ['reg_date', 'dob']}
VOTED_VALUES = 'voted_values'
VOTE_HISTORIES = 'vote_histories'
ELEMENTS_REQ_IN_VALUES_VF = [VOTED_VALUES, 'parties_included']
VHIST_DATE_FORMAT = '%m-%d-%Y'
SET_REQ_FIELDS = set(REQUIRED_FIELDS_IN_CONFIG.keys() + [VOTE_HISTORIES, VOTED_VALUES])

class TurnoutConfig(object):
    """Creates and validates the config used to compute turnout """
    def __init__(self, config):
        self.config = config
        self._validated_config()
        self._voter_file = None
        self._s3_creds = None
        self._column_names = None
        self._formats = None
        self._parties_included = None
        self._filter_on_parties = True
        self.voted_values = self.config['voted_values']

    @property
    def parties_included(self):
        """ confirms whether the field exists and sets the list of parties to filter on """
        if self._parties_included is None and self._filter_on_parties is True:
            self._parties_included = self.config.get('parties_included')
            if self._parties_included is None:
                logging.warn("""config did not specify the list of parties to include,
                            "parties_included" is the correct key to specify list of parties
                            to include """)
                self._filter_on_parties = False
            elif len(self._parties_included) == 0:
                logging.warn("""you are trying to filter on party but provided an empty list
                                will not filter on party.""")
                self._parties_included = None
                self._filter_on_parties = False

        return self._parties_included

    @property
    def voter_file(self):
        """ the bucket and key where voter file is in s3"""
        if self._voter_file is None:
            self._voter_file = VoterFilePath(self.config['voter_file'])
        return self._voter_file

    @property
    def column_names(self):
        """ names of the columns in voter file """
        if self._column_names is None:
            self._column_names = ColumnNames(self.config['column_names'])
        return self._column_names

    @property
    def formats(self):
        """ date formats """
        if self._formats is None:
            self._formats = DateFormats(self.config['formats'])
        return self._formats

    @property
    def s3_creds(self):
        """ confirm type of s3_creds and """
        if self._s3_creds is None:
            if self.config.get('s3_creds') is None:
                raise KeyError("""you need s3_creds in the config""")
            self._s3_creds = self.config['s3_creds']
            if not isinstance(self._s3_creds, dict):
                raise TypeError(""" s3_creds should be a dictionary with the s3 credentials """)
        return self._s3_creds

    def _validated_config(self):
        # check that all the required fields are in config (includes voted_values and histories)
        config_keys = set(self.config.keys())
        if SET_REQ_FIELDS.intersection(config_keys) != SET_REQ_FIELDS:
            err_msg = """ required field(s) but not available in your config """
            raise KeyError(', '.join(list(SET_REQ_FIELDS - config_keys)) + err_msg)

        # check that all the REQUIRED_FIELDS_IN_CONFIG are dict
        for req_field, vlues in REQUIRED_FIELDS_IN_CONFIG.iteritems():
            if not isinstance(self.config[req_field], dict):
                err_msg = """{req_field} in the config is supposed to be a
                         dictionary""".format(req_field=req_field)
                raise TypeError(err_msg)
            set_cfg_sub = set(self.config[req_field].keys())
            if set_cfg_sub != set(vlues):
                err_msg = """ required sub field(s) of {req_field} but not available
                     in your config""".format(req_field=req_field)
                raise KeyError(', '.join(list(set(vlues) - set_cfg_sub)) + str(err_msg))

        # check that VOTE_HISTORIES is a dictionary
        if not isinstance(self.config[VOTE_HISTORIES], dict):
            err_msg = """{vote_histories} in the config is supposed to be a
                         dictionary""".format(vote_histories=VOTE_HISTORIES)
            raise TypeError(err_msg)

        # check that all the VOTE_HISTORIES elections are available valid columns
        for elec, elecdate in self.config[VOTE_HISTORIES].iteritems():
            try:
                datetime.datetime.strptime(str(elecdate), VHIST_DATE_FORMAT)
            except ValueError:
                err_msg = """date format for {elec} in voter_history is not in required
                             format: {req_format}.""".format(elec=elec,
                                                             req_format=VHIST_DATE_FORMAT)
                raise ValueError(err_msg)

        #check that ELEMENTS_REQ_IN_VALUES_VF are list if available
        #some are required to be available and are in SET_REQ_FIELDS but not all
        for elms_req in ELEMENTS_REQ_IN_VALUES_VF:
            if self.config.get(elms_req) is not None:
                if not isinstance(self.config[elms_req], list):
                    err_msg = """{elms_req} in the config is supposed to be a
                                 list""".format(elms_req=elms_req)
                    raise TypeError(err_msg)

    def validate_against_voter_file(self, voter_history_df):
        """ given a vf, make sure basic columns are there """
        df_columns = voter_history_df.columns
        clm_names = self.config[VOTE_HISTORIES].keys()
        for clm_name in clm_names:
            if clm_name not in df_columns:
                err_msg = """{clm_name} specified in vote_histories in config is not
                             a valid column name in the dataframe
                             """.format(clm_name=clm_name)
                raise KeyError(err_msg)

        #for elm in ELEMENTS_REQ_IN_VALUES_VF:
        for req_column in REQUIRED_FIELDS_IN_CONFIG['column_names']:
            req_clm_name = getattr(self.column_names, req_column)
            if req_clm_name not in df_columns:
                err_msg = """{req_clm_name} specified for {req_column} in config is not a valid
                             column name in the dataframe""".format(req_clm_name=req_clm_name,
                                                                    req_column=req_column)
                raise KeyError(err_msg)

    def columns_to_keep(self):
        """ returns the columns required for the turnout computation """
        columns_to_keep = []
        for req_column in REQUIRED_FIELDS_IN_CONFIG['column_names']:
            req_clm_name = getattr(self.column_names, req_column)
            columns_to_keep.append(req_clm_name)

        clm_names = self.config[VOTE_HISTORIES].keys()
        columns_to_keep.extend(clm_names)
        return columns_to_keep

class ColumnNames(object):
    def __init__(self, config_col_names):
        self.config_col_names = config_col_names
        self._dob_field = None
        self._id_field = None
        self._party_field = None
        self._reg_date_field = None

    @property
    def dob_field(self):
        if self._dob_field is None:
            self._dob_field = self.config_col_names['dob_field']
        return self._dob_field


    @property
    def id_field(self):
        if self._id_field is None:
            self._id_field = self.config_col_names['id_field']
        return self._id_field

    @property
    def party_field(self):
        if self._party_field is None:
            self._party_field = self.config_col_names['party_field']
        return self._party_field

    @property
    def reg_date_field(self):
        if self._reg_date_field is None:
            self._reg_date_field = self.config_col_names['reg_date_field']
        return self._reg_date_field

class DateFormats(object):
    def __init__(self, config_date_formats):
        self.config_date_formats = config_date_formats
        self._dob = None
        self._reg_date = None

    @property
    def reg_date(self):
        if self._reg_date is None:
            self._reg_date = str(self.config_date_formats['reg_date'])
        return self._reg_date

    @property
    def dob(self):
        if self._dob is None:
            self._dob = str(self.config_date_formats['dob'])
        return self._dob

class VoterFilePath(object):
    def __init__(self, config_voter_file):
        self.config_voter_file = config_voter_file
        self._bucket = None
        self._key = None

    @property
    def bucket(self):
        if self._bucket is None:
            self._bucket = self.config_voter_file['bucket']
        return self._bucket

    @property
    def key(self):
        if self._key is None:
            self._key = self.config_voter_file['key_pattern']
        return self._key
