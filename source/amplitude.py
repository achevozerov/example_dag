import pandas as pd
import requests as rq
import io
import zipfile
import gzip
import json
import logging

from requests.auth import HTTPBasicAuth
from source.clickhouse_tools import ClickhouseConnect
from datetime import datetime, timedelta


class Amplitude_etl_cls(ClickhouseConnect):
    def __init__(self, target_db: str, target_table: str, file_path: str, key: str, secret_key: str, date_from: str, date_to: str):
        self.target_db = target_db
        self.target_table = target_table
        self.date_from = date_from
        self.date_to = date_to

        self.TEMP_PATH = file_path
        self.api_key = key
        self.api_secret_key = secret_key

    def download_amplitude(self):
        """
            Метод получает данные за определённый период из Amplitude API и записывает их в файл
        """
        logging.info(f'Загрузка данных с {self.date_from} по {self.date_to}')
        params = {
                "start": f"{self.date_from}",
                "end": f"{self.date_to}"
            }

        response = rq.get("http://amplitude.com/api/2/export", params=params,
                          auth=HTTPBasicAuth(self.api_key, self.api_secret_key),
                          stream=True)

        if response.status_code == 200:
            zf = zipfile.ZipFile(io.BytesIO(response.content), 'r')
            logging.info('Файл загружен, начинаем декодирование')
        else:
            raise Exception(f"Сервер вернул статус {response.status_code}")

        json_list = list()
        for gz_file in zf.filelist:
            js_rows = gzip.decompress(zf.read(gz_file.filename)).decode('utf8')
            for s in js_rows.split('\n'):
                if s:
                    json_list.append(json.loads(s))

        logging.info("Декодирование завершено")

        with open(self.TEMP_PATH, "w") as f:
            for i in json_list:
                json.dump(i, f)
                f.write('\n')

        logging.info("Файл записан на диск")

    def upload_data(self):
        """
            Метод считывает данные из файла и отправляет их в clickhouse
        """
        logging.info("Считываем файл в датафрейм")
        chunks = pd.read_json(self.TEMP_PATH, lines=True, chunksize=100000)

        for df in chunks:
            new_cols = list()
            for col in df.columns:
                new_cols.append(col.replace("$", ""))

            df.columns = new_cols
            dicts_list = ['event_properties', 'user_properties', 'global_user_properties', 'group_properties', 'data', 'groups', 'amplitude_attribution_ids', 'plan', 'is_attribution_event']
            for i in dicts_list:
                df[i] = df[i].astype('string')

            df['partner_id'] = df['partner_id'].fillna(0).astype("int")
            df['server_upload_date'] = df['server_upload_time'].apply(lambda x: x.date())

            na_dict = {
                "amplitude_attribution_ids": ''
            }

            df = df.fillna(na_dict)

            logging.info("Предобработка данных завершена")
            self._connect_to_clickhouse()
            self.ch_client.execute(f'INSERT INTO {self.target_db}.{self.target_table} VALUES', df.to_dict('records'), types_check=True)
            logging.info("Чанк загружен")
        logging.info("Загрузка данных завершена")


def wrapper_amplitude_streaming(dt: str, file_path: str, key: str, secret_key: str):
    """
        Функция управляет процессом выгрузки данных по API и загрузки их в слой сырых данных
    """
    date = datetime.strptime(dt,"%Y%m%dT%H%M%S")
    date_from = (date + timedelta(hours=3) - timedelta(hours=12)).strftime('%Y%m%dT%H')
    date_to = (date + timedelta(hours=3) - timedelta(hours=7)).strftime('%Y%m%dT%H')

    logging.info(f'Запущена выгрузка с {date_from} по {date_to}')

    ampl_cls = Amplitude_etl_cls("rdl_amplitude", "amplitude_streaming", file_path, key, secret_key, date_from, date_to)
    ampl_cls.download_amplitude()
    ampl_cls.upload_data()
