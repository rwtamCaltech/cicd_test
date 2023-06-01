from Constant import DATABASE_NAME, TABLE_NAME, ONE_GB_IN_BYTES, QUERY_COST_PER_GB_IN_DOLLARS
from contexttimer import Timer
from quakes2aws_datastore.logging import logger  # noqa:E402

class QueryExample:
    # HOSTNAME = "host-24Gju"

    def __init__(self, client):
        self.client = client
        self.paginator = client.get_paginator('query')

    # See records ingested into this table so far
    SELECT_ALL = f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}"


    #QUERY RT specialized; SELECT * FROM devops_rt.host_metrics_rt WHERE Startt='1649776945' ORDER BY time ASC LIMIT 10
    '''
    The following two are older, when we used SampRate as our measure_name
    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_name = 'SampRate' AND measure_value::double=100.0 AND Startt='1649776945') ORDER BY time ASC LIMIT 10
    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_name = 'SampRate' AND measure_value::double=100.0) ORDER BY time

    The following two are newer:
    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_name = 'Startt' AND measure_value::double>1649776945 AND measure_value::double<1649776955) ORDER BY time
    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_name = 'Startt' AND measure_value::double>1649776943 AND measure_value::double<1649776976) ORDER BY time

    EVEN NEWER:
    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_name = 'Startt' AND measure_value::double>1673293594 AND measure_value::double<1673293626) ORDER BY time
    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_value::double>1673293594 AND measure_value::double<1673293626) ORDER BY time


    SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_value::double>1673316106 AND measure_value::double<1673316137) ORDER BY time
    ^^This data got deleted already, because only gets stored in the magnetic storage for a full day.


    Can also find the maximum value of our DB:
    SELECT MAX(measure_value::double) FROM devops_rt.host_metrics_rt
    -->That value is 1.673316236E9  as of 1/10/23
    '''
    #^^ABOVE WORKS; but below we take the starttimes to be different. I think this is the problem. measure_value::double reflects startt, and we get fluctuating results out of it.
    #We need to get the endt
    #16733161669.68392
    QUERY_RT=f"""
        SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_value::double>1673470083 AND measure_value::double<1673470114) ORDER BY time
    """

    # QUERY_RT=f"""
    #     SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_name = 'Startt' AND measure_value::double>1649776943 AND measure_value::double<1649776976) ORDER BY time
    # """

    #1/5/23 ADDED run_rt_query
    # def run_rt_query(self):
    def run_rt_query(self,query_specified):
        try:
            # page_iterator = self.paginator.paginate(QueryString=self.QUERY_RT) #page_iterator has no size, and this is instantaneous to run
            page_iterator = self.paginator.paginate(QueryString=query_specified) #page_iterator has no size, and this is instantaneous to run

            all_query_results=[]

            '''
            Time to run a page section (the whole paging below takes 41 seconds, but multiprocessing paging does not work either)
            41.15
            Similar prob to this: https://github.com/aws/aws-sdk-pandas/issues/1308
            '''
            with Timer() as paginate_time:
                gbytes_scanned_tot=0
                gbytes_metered_tot=0
                cost_for_query=0

                for page in page_iterator: 
                    query_status = page["QueryStatus"]

                    # progress_percentage = query_status["ProgressPercentage"]
                    # print(f"Query progress so far: {progress_percentage}%")

                    gigabytes_scanned = float(query_status["CumulativeBytesScanned"]) / ONE_GB_IN_BYTES
                    gbytes_scanned_tot+=gigabytes_scanned 
                    # print(f"Data scanned so far: {bytes_scanned} GB")

                    gigabytes_metered = float(query_status["CumulativeBytesMetered"]) / ONE_GB_IN_BYTES
                    cost_added=gigabytes_metered*QUERY_COST_PER_GB_IN_DOLLARS
                    gbytes_metered_tot+=gigabytes_metered
                    cost_for_query+=cost_added
                    # print(f"Data metered so far: {bytes_metered} GB")

                    column_info = page['ColumnInfo'] 

                    # print("Metadata: %s" % column_info)
                    # print("Data: ")
                    for row in page['Rows']: #for each row of data
                        '''
                        RT 1/10/23: This iterates across each of the dictionaries we have here:
                        [{'ScalarValue': 'RSS'}, {'NullValue': True}, {'ScalarValue': 'HNE'}, {'NullValue': True}, {'ScalarValue': 'CI'}, {'ScalarValue': '1673293596.958392'}, 
                        {'ScalarValue': '[-1296...}]
                        '''
                        data = row['Data']

                        # print("Data")
                        # print(data)


                        row_output = {}
                        for j in range(len(data)): #in the example above, this iterates across each datum; so first, it goes to scalar value, then it goes to nullValue
                            #However, we don't care about the null value, so we continue if that's what we see
                            info = column_info[j]
                            datum = data[j]

                            # print("Datum")
                            # print(datum)

                            #RT 1/10/23 update:
                            try:
                                row_output[info['Name']]=datum['ScalarValue']
                            except: #our later data has NullValue; we ignore NullValues, we don't need them
                                continue 

                            #RT change the below to a dict form. 
                            # row_output.append(self._parse_datum(info, datum))
                        
                        #This is how we get the list of dicts for that particular instance
                        all_query_results.append(row_output)
                    # self._parse_query_result(page) #PREVIOUSLY
                
            # paginate_time_elapsed=round(paginate_time.elapsed,3)
            # print("Time to run a page section")
            # print(paginate_time_elapsed)
            return all_query_results,gbytes_scanned_tot,gbytes_metered_tot,cost_for_query #return all desired results
        except Exception as err:
            logger.info('maxtime_query.exception',error=str(err))
            return 0


    #May not even need a function for this. 
    # #1/6/23 I'm going to create a new function instead of parse_datum below, where I will account for the exception cases:
    # def _parse_datum_rt(self, info, datum):
    #     # column_type = info['Type'] #may not be needed
    #     #Add more after lunch.
    #     #Instead of something like this: self._parse_column_name(info) + datum['ScalarValue']
    #     #We can create a dictionary of values associating info['Name'] with datum['ScalarValue'] at the end of the dictionary.
    #     # info['Name']

    def _parse_query_result(self, query_result):
        query_status = query_result["QueryStatus"]

        progress_percentage = query_status["ProgressPercentage"]
        print(f"Query progress so far: {progress_percentage}%")

        bytes_scanned = float(query_status["CumulativeBytesScanned"]) / ONE_GB_IN_BYTES
        print(f"Data scanned so far: {bytes_scanned} GB")

        bytes_metered = float(query_status["CumulativeBytesMetered"]) / ONE_GB_IN_BYTES
        print(f"Data metered so far: {bytes_metered} GB")

        column_info = query_result['ColumnInfo']

        print("Metadata: %s" % column_info)
        print("Data: ")
        for row in query_result['Rows']:
            print(self._parse_row(column_info, row))

    def _parse_row(self, column_info, row):
        data = row['Data']
        row_output = []
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(self._parse_datum(info, datum))

        return "{%s}" % str(row_output)

    def _parse_datum(self, info, datum):
        if datum.get('NullValue', False):
            return "%s=NULL" % info['Name'],

        column_type = info['Type']

        # If the column is of TimeSeries Type
        if 'TimeSeriesMeasureValueColumnInfo' in column_type:
            return self._parse_time_series(info, datum)

        # If the column is of Array Type
        elif 'ArrayColumnInfo' in column_type:
            array_values = datum['ArrayValue']
            return "%s=%s" % (info['Name'], self._parse_array(info['Type']['ArrayColumnInfo'], array_values))

        # If the column is of Row Type
        elif 'RowColumnInfo' in column_type:
            row_column_info = info['Type']['RowColumnInfo']
            row_values = datum['RowValue']
            return self._parse_row(row_column_info, row_values)

        # If the column is of Scalar Type
        else:
            return self._parse_column_name(info) + datum['ScalarValue']

    def _parse_time_series(self, info, datum):
        time_series_output = []
        for data_point in datum['TimeSeriesValue']:
            time_series_output.append("{time=%s, value=%s}"
                                      % (data_point['Time'],
                                         self._parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],
                                                           data_point['Value'])))
        return "[%s]" % str(time_series_output)

    def _parse_array(self, array_column_info, array_values):
        array_output = []
        for datum in array_values:
            array_output.append(self._parse_datum(array_column_info, datum))

        return "[%s]" % str(array_output)

    def run_query_with_multiple_pages(self, limit):
        query_with_limit = self.SELECT_ALL + " LIMIT " + str(limit)
        print("Starting query with multiple pages : " + query_with_limit)
        self.run_query(query_with_limit)

    def cancel_query(self):
        print("Starting query: " + self.SELECT_ALL)
        result = self.client.query(QueryString=self.SELECT_ALL)
        print("Cancelling query: " + self.SELECT_ALL)
        try:
            self.client.cancel_query(QueryId=result['QueryId'])
            print("Query has been successfully cancelled")
        except Exception as err:
            print("Cancelling query failed:", err)

    @staticmethod
    def _parse_column_name(info):
        if 'Name' in info:
            return info['Name'] + "="
        else:
            return ""
