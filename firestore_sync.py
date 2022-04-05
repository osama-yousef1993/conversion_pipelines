"""Example of job using custom DoFn to upload data to Firestore Native using Firestore Client library"""

from datetime import datetime, timedelta
import apache_beam as beam
from google.cloud import firestore
from google.cloud import storage
import pandas as pd
import io
import argparse
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions


class ReadFile(beam.DoFn):
    def process(self, file_name):
        from apache_beam.io.filesystems import FileSystems
        reader = FileSystems.open(path=file_name)
        df = pd.read_csv(io.StringIO(reader.read().decode('utf-8')), sep='\t', header=None)
        df.columns = ['impression_timestamp_gmt', 'event_timestamp_gmt', 'event_report_timestamp', 'imp_auction_id', 'mm_uuid', 'organization_id', 'organization_name', 'agency_id',
                      'agency_name', 'advertiser_id', 'advertiser_name', 'event_type', 'pixel_id', 'pixel_name', 'pv_pc_flag', 'pv_time_lag', 'pc_time_lag', 'campaign_id',
                      'campaign_name', 'strategy_id', 'strategy_name', 'concept_id', 'concept_name', 'creative_id', 'creative_name', 'exchange_id', 'exchange_name', 'width',
                      'height', 'site_url', 'mm_v1', 'mm_v2', 'mm_s1', 'mm_s2', 'day_of_week', 'week_hour_part', 'mm_creative_size', 'placement_id', 'deal_id', 'country_id',
                      'country', 'region_id', 'region', 'dma_id', 'dma', 'zip_code_id', 'zip_code', 'conn_speed_id', 'conn_speed', 'isp_id',
                      'isp', 'category_id', 'publisher_id', 'site_id', 'watermark', 'fold_position', 'user_frequency', 'browser_id', 'browser', 'os_id', 'os',
                      'browser_language_id', 'week_part', 'day_part', 'day_hour', 'week_part_hour', 'hour_part', 'week_part_hour_part', 'week_hour', 'batch_id',
                      'browser_language', 'empty_int_1', 'homebiz_type_id', 'homebiz_type', 'inventory_type_id', 'inventory_type', 'device_type_id', 'device_type', 'connected_id',
                      'app_id', 'event_subtype', 'city', 'city_code', 'city_code_id', 'supply_source_id', 'ip_address', 'browser_name', 'browser_version', 'os_name', 'os_version',
                      'model_name', 'brand_name', 'form_factor', 'impressions_stream_uuid', 'clicks', 'pc_conversions', 'pv_conversions', 'pc_revenue', 'pv_revenue',
                      'unkownn_additional_col1', 'unkownn_additional_col2']
        df['impression_timestamp_gmt'] = pd.to_datetime(df['impression_timestamp_gmt'])
        df['event_timestamp_gmt'] = pd.to_datetime(df['event_timestamp_gmt'])
        df['event_report_timestamp'] = pd.to_datetime(df['event_report_timestamp'])
        logging.getLogger().info('number of lines in file >>> {file_name}  >>>> number of line >>>>>>>> ', df.shape[0])
        for index, rows in df.iterrows():
            yield rows


class FirestoreWriteDoFn(beam.DoFn):
    MAX_DOCUMENTS = 200

    def __init__(self, project, collection):
        self._project = project
        self._collection = collection

    def start_bundle(self):
        self._mutations = []

    def finish_bundle(self):
        if self._mutations:
            self._flush_batch()

    def process(self, element):
        self._mutations.append(element)
        if len(self._mutations) > self.MAX_DOCUMENTS:
            self._flush_batch()

    def _flush_batch(self):
        db = firestore.Client(project=self._project)
        print(db)
        batch = db.batch()
        collection = db.collection(self._collection)
        for mutation in self._mutations:
            row_key = f"{mutation['advertiser_id']},{mutation['campaign_id']},{mutation['pixel_id']},{mutation['impression_timestamp_gmt']},{mutation['event_report_timestamp']},{mutation['event_timestamp_gmt']},{mutation['mm_uuid']},{mutation['pv_pc_flag']}, {mutation['imp_auction_id']},{mutation['pc_conversions']},{mutation['pv_conversions']}"
            dict_row = mutation.to_dict()
            if len(self._mutations) == 1:
                # autogenerate document_id
                ref = collection.document(row_key)
                ref.set(dict_row)
            else:
                ref = collection.document(row_key)
                ref.set(dict_row)
            batch.commit()
        self._mutations = []


class ConversionsFiles():
    def __init__(self, project):
        self._project = project
        self.client = storage.Client(project=self._project)

    def list_files_for_all_convertions(self, date):
        prefix = os.path.join('MediaMath/log-level/attributed-events', date, 'conversion')
        blobs = self.client.list_blobs(
            'at-datalake-staging',
            prefix=prefix,
            end_offset='txt')
        files = list()
        for page in blobs.pages:
            for f in page:
                if '.log' not in f.name:
                    files.append(f'gs://at-datalake-staging/{f.name}')
        files.remove(files[0])
        return files


def dataflow(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default=None, help="Input file to download")
    parser.add_argument('--collection_name', dest='collection_name', default=None, help="collection name for add data to firestore collection")
    parser.add_argument('--project_id', dest='project_id', default=None, help="project id for GCP account")
    parser.add_argument('--service_account', dest='service_account', default=None, help="default service account")

    known_args, pipeline_options = parser.parse_known_args(argv)
    JOB_NAME = 'firestore-upload-{}'.format(datetime.now().strftime('%Y-%m-%d-%H%M%S'))
    files_list = []
    conversions_handler = ConversionsFiles(known_args.project_id)
    for i in range(346):
        file_date = (datetime.utcnow() - timedelta(i + 1)).strftime('%Y/%m/%d')
        get_files = conversions_handler.list_files_for_all_convertions(file_date)
        files_list.extend(get_files)

    beam_option = PipelineOptions(
        region='us-east4',
        runner='DataflowRunner',  # run on dataflow
        # runner='DirectRunner',  # run on local machine
        project=known_args.project_id,
        job_name=JOB_NAME,
        temp_location='gs://at-datalake-staging/ConversionPipelines/beam',
        staging_location='gs://at-datalake-staging/ConversionPipelines/test/beam',
        requirements_file='./requirements.txt',
    )
    beam_option.view_as(SetupOptions).save_main_session = True
    google_cloud_options = beam_option.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project_id
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.service_account_email = known_args.service_account
    with beam.Pipeline(options=beam_option) as p:
        (p
         | 'Reading input file' >> beam.Create(files_list)
         | "read file from GCS" >> beam.ParDo(ReadFile())
         | 'Write entities into Firestore' >> beam.ParDo(FirestoreWriteDoFn(known_args.project_id, known_args.collection_name))
         )


if __name__ == '__main__':
    dataflow()
