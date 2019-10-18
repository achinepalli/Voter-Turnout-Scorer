"""
Tunrnout Scorer creates the turnout score based on historic data
about tunrout from the voter file
"""
import datetime
import logging
from py_common.storage.spark_s3 import SparkS3
from py_common.util.spark import generate_temptable_name
from ac_political.turnout_scorer.turnout_config import (TurnoutConfig, VOTE_HISTORIES,
                                                        VHIST_DATE_FORMAT)
from pyspark.sql.types import IntegerType, StringType, FloatType

from pyspark.sql.functions import udf, array, col

EFF_REG_DATE_FIELD = 'effective_reg_date'
WEIGHTED_TURNOUT_FIELD = 'weighted_turnout'
YEAR_OF_BIRTH_FIELD = 'year_of_birth'
NORMALIZED_TURNOUT_FIELD = 'normalized_turnout'


class TurnoutScorer(object):
    """Creates a turnout score based on historic election turnout and
       imputes recent registerants' scores using bayesian inference """

    def __init__(self, config, sql_ctxt, num_valid=6):
        self.config = TurnoutConfig(config)
        self.sql_ctxt = sql_ctxt
        self.spark_s3 = SparkS3(self.config.s3_creds, sql_ctxt)
        self.vote_histories = config[VOTE_HISTORIES]
        self._sorted_vote_histories = None
        self.num_valid = num_valid
        self.voters_df = self._preprocess_vf()

    @property
    def sorted_vote_histories(self):
        """ list of elections in order from oldest to mosts recent """
        if self._sorted_vote_histories is None:
            self._sorted_vote_histories = self._sort_vote_histories()
        return self._sorted_vote_histories

    def _sort_vote_histories(self):
        dct = {key: datetime.datetime.strptime(value, VHIST_DATE_FORMAT)
               for key, value in self.vote_histories.iteritems()}
        sorted_histories = sorted(dct, key=dct.get)
        infomsg = ('Confirm that this is the order of the histories: ' +
                   ', '.join(sorted_histories))
        logging.warn(infomsg)
        return sorted_histories


    def _preprocess_vf(self):
        voter_history_df = self.spark_s3.load_csv(self.config.voter_file.bucket,
                                                  self.config.voter_file.key)
        self.config.validate_against_voter_file(voter_history_df)
        columns_to_keep = self.config.columns_to_keep()

        vdf = voter_history_df[columns_to_keep]
        logging.info('Dataframe is of length ' + str(vdf.count()))
        if self.config.parties_included is not None:
            parties_to_include = self.config.parties_included
            party_field = self.config.column_names.party_field
            vdf = vdf[vdf[party_field].isin(parties_to_include)]
            logging.info('Dataframe is of length ' + str(vdf.count()) + ' after filtering')

        count_vdf = vdf.count()
        if count_vdf == 0:
            raise ValueError(','.join(parties_to_include) + """ not valid party value(s)
                             in {party_field}""".format(party_field=party_field))

        if vdf.distinct().count() != count_vdf:
            raise RuntimeError("""There are duplicates in the voterfile. Please fix and rerun""")
        dob_format = self.config.formats.dob
        dob_field = self.config.column_names.dob_field

        def get_year_ob(date_input):
            """ get year from date of birth """
            if date_input not in (None, 'null'):
                return str(datetime.datetime.strptime(date_input, dob_format).year)
            else:
                return str(datetime.date.today().year - 18)

        get_year_udf = udf(lambda x: get_year_ob(x), StringType())
        vdf = vdf.withColumn(YEAR_OF_BIRTH_FIELD, get_year_udf(col(dob_field)))
        return vdf

    def _add_effective_reg_date(self):
        array_ordered_elections = array(self.sorted_vote_histories)
        svh = self.sorted_vote_histories
        vhs_w_d = self.vote_histories
        reg_date_format = self.config.formats.reg_date
        def effective_regis_date(arr_of_elec):
            """ date of first recorded elections """
            first_election_date = datetime.date.today()
            for idx, i in enumerate(arr_of_elec):
                if i not in (None, 'null'):
                    first_election_date = datetime.datetime.strptime(vhs_w_d[svh[idx]],
                                                                     VHIST_DATE_FORMAT)
                    break
            return str(first_election_date.strftime(reg_date_format))
        effective_regis_dateudf = udf(lambda arr: effective_regis_date(arr), StringType())
        self.voters_df = self.voters_df.withColumn(EFF_REG_DATE_FIELD,
                                                   effective_regis_dateudf(array_ordered_elections))


    def _register_clean_voting_records(self):
        self._add_effective_reg_date()
        reg_date_format = self.config.formats.reg_date
        voted_values = self.config.voted_values
        def clean_voting_record(vote, eff_reg_date, reg_date, elections_date):
            """ udf to be used in sql to clean records
                records are all turned to 1, 0 if eligible or
                null (if not eligible or missing registeration date)
                use registeration date for eligibility unless there
                is data prior to that in which case we use effective
                reg which is date if first elections with data """
            if reg_date in (None, 'null'):
                reg_date = datetime.date.today()


            elections_date = datetime.datetime.strptime(str(elections_date),
                                                        VHIST_DATE_FORMAT)
            try:
                reg_date = datetime.datetime.strptime(str(reg_date), reg_date_format)
                eff_reg_date = datetime.datetime.strptime(str(eff_reg_date), reg_date_format)

            except ValueError:
                return None

            if eff_reg_date < reg_date:
                reg_date = eff_reg_date

            if reg_date <= elections_date:
                if vote in voted_values:
                    return 1
                else:
                    return 0
            else:
                return None
        self.sql_ctxt.udf.register("clean_voting_record", clean_voting_record, IntegerType())

    def _clean_voting_record(self):
        """ adds a column for cleaned vote history """
        self._register_clean_voting_records()

        voters_table = generate_temptable_name(self, 'vtr_table')
        self.voters_df.registerTempTable(voters_table)
        reg_date_field = self.config.column_names.reg_date_field

        elections = self.vote_histories
        generated_cleaned_fields = ', '.join([
            """clean_voting_record(`{election}`, `{eff_reg_date}`,"""
            """ `{reg_date_field}`, "{election_date}") as `{election}_cleaned`""".format(
                election=election,
                eff_reg_date=EFF_REG_DATE_FIELD,
                reg_date_field=reg_date_field,
                election_date=election_date)
            for election, election_date in elections.iteritems()])

        sql_parameters = {'generated_cleaned_fields' : generated_cleaned_fields,
                          'voters_table': voters_table}
        self.voters_df = self.sql_ctxt.sql(
            """
            SELECT
              *,
              {generated_cleaned_fields}
            FROM {voters_table}
            """.format(**sql_parameters))

    def _add_weighted_vote(self):
        voters_table = generate_temptable_name(self, 'vtr_table_to_norm')
        self.voters_df.registerTempTable(voters_table)
        elections = [vote + '_cleaned' for vote in self.sorted_vote_histories]

        for election in elections:
            nrm_fac = self.sql_ctxt.sql(
                """
                    SELECT
                      SUM(`{election}`)/COUNT(`{election}`) AS norm_fac
                    FROM
                    {voters_table}
                    WHERE `{election}` IS NOT NULL

                """.format(election=election,
                          voters_table=voters_table)).first()['norm_fac']
            weighted_col_name = election.split('_cleaned')[0]+'_weighted'
            self.voters_df = self.voters_df.withColumn(weighted_col_name,
                                                       col(election)*(1. - nrm_fac))


    ## TODO: if an election has 0 turnout for everyone, throw a warning/error
    def _add_weighted_turnout(self):
        self._add_weighted_vote()

        weightes_vote_fields = [vote + '_weighted' for vote in self.sorted_vote_histories]
        ordered_historic_elections = array(weightes_vote_fields)

        def get_weighted_sum(arr_of_elec):
            """ sum the weighted turnout for each person """
            return float(sum(filter(lambda v: v not in (None, "null"), arr_of_elec)))
        get_weighted_sumudf = udf(lambda arr: get_weighted_sum(arr), FloatType())
        self.voters_df = self.voters_df.withColumn(WEIGHTED_TURNOUT_FIELD,
                                                   get_weighted_sumudf(ordered_historic_elections))

    def _normalize_turnout(self):
        cohort_field = EFF_REG_DATE_FIELD # to be moved to config (could be birth year, regdate, ..)
        voters_table = generate_temptable_name(self, 'vtr_table')
        self.voters_df.registerTempTable(voters_table)

        sql_parameters = {
            'weighted_turnout': WEIGHTED_TURNOUT_FIELD,
            'cohort_field': cohort_field,
            'voters_table': voters_table,
            'normalize_turnout': NORMALIZED_TURNOUT_FIELD
        }
        sql = """
            WITH max_weight_cohort AS(
                SELECT
                 *,
                 MAX({weighted_turnout}) OVER (PARTITION BY `{cohort_field}`) AS max_to
                FROM
                {voters_table})
            SELECT
              *,
              {weighted_turnout} / max_to AS {normalize_turnout}
              FROM max_weight_cohort
        """.format(**sql_parameters)

        self.voters_df = self.sql_ctxt.sql(sql).drop('max_to')

    def compute_normalized_turnout(self):
        """ puts it all together """
        self._clean_voting_record()
        self._add_weighted_turnout()
        self._normalize_turnout()
