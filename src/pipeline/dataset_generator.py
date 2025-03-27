import json
import random
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.utils.logging import setup_logging

logger = setup_logging(__name__)

np.random.seed(42)

@dataclass
class NPSDatasetGenerator:
    data_folder: str = "data"
    num_observations: int = 1000
    min_date: str='2020-01-01'
    max_date: str='2024-12-31'
    nan_fraction: float=0.02

    def __post_init__(self):
        self.data_folder = Path(self.data_folder)
        self.in_data_folder = self.data_folder / 'raw'
        self.out_data_folder = self.data_folder / 'interim'
        self.out_path = self.out_data_folder / "nps_data.csv"

        with open(self.in_data_folder / 'segments', 'r', encoding='utf-8') as file:
            self.segments = [line.strip() for line in file]

        with open(self.in_data_folder / "segments_description.json", "r", encoding="utf-8") as file:
            self.segments_description = json.load(file)

        self.df = pd.DataFrame()
    
    def _generate_date(self) -> datetime:

        min_dt = datetime.strptime(self.min_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        max_dt = datetime.strptime(self.max_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")

        random_seconds = random.randint(0, int((max_dt - min_dt).total_seconds()))
        random_dt = min_dt + timedelta(seconds=random_seconds)
        
        return random_dt

    def _get_time_group(self, timestamp: datetime) -> str:
        hour = timestamp.hour
        if 6 <= hour < 12:
            return 'Утро'
        elif 12 <= hour < 18:
            return 'День'
        elif 18 <= hour < 22:
            return 'Вечер'
        else:
            return 'Ночь'
    
    def _generate_values_with_nan(self, values, nan_fraction: float=None) -> np.ndarray:
        if nan_fraction is None:
            nan_fraction = self.nan_fraction
    
        p_uniform = (1 - nan_fraction) / len(values)
        probas = [p_uniform] * len(values) + [nan_fraction]
        return np.random.choice(values + [None], p=probas, size=self.num_observations)

    def generate_date_columns(self):
        self.df['SNAP_DATE'] = [self._generate_date() for _ in range(self.num_observations)]
        self.df["SNAP_WEEK"] = self.df['SNAP_DATE'].dt.isocalendar().week
        self.df["SNAP_MONTH"] = self.df['SNAP_DATE'].dt.month
        self.df["SNAP_QUARTER"] = self.df['SNAP_DATE'].dt.quarter

        self.df['SURVEY_TIME_GROUP'] = self.df['SNAP_DATE'].apply(self._get_time_group)
        self.df['SURVEY_DAY'] = self.df['SNAP_DATE'].dt.day

        weekdays = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        self.df['SURVEY_WEEKDAY'] = self.df['SNAP_DATE'].dt.dayofweek.apply(lambda x: weekdays[x])

        self.df['SURVEY_WEEKDAY_TYPE'] = self.df['SURVEY_WEEKDAY'].apply(lambda x: 'Выходной' if x in ['Суббота', 'Воскресенье'] else 'Будний')

    def generate_ids(self):
        # self.df['MEASURE_ID'] = np.random.randint(1, 100, self.num_observations)
        self.df['SK_SUBS_ID'] = np.random.randint(1000, 9999, self.num_observations)
        self.df['MNCO_GROUP_ID'] = np.random.randint(1, 10, self.num_observations)
        self.df['OS_OS_ID'] = np.random.randint(1, 50, self.num_observations)
        self.df['FILIAL_ID'] = np.random.randint(1, 20, self.num_observations)
    
    def generate_personal_info(self):
        self.df['SURVEY_AGE'] = np.random.randint(18, 90, size=self.num_observations)  #TODO: to cat '18-24', '25-34', '35-44', '45-54', '55+'
        self.df['SURVEY_GENDER'] = self._generate_values_with_nan(['Муж', 'Жен', "Не определен"])
        self.df['SURVEY_ARPU_GROUP'] = self._generate_values_with_nan(['Высокий', 'Средний', 'Низкий'], nan_fraction=0.0)  #TODO: weights
        self.df['SURVEY_CITY_SIZE'] = self._generate_values_with_nan(['Большой', 'Средний', 'Малый'])  #TODO: weights
    
    def generate_operator_info(self):
        self.df['SURVEY_OPERATOR_AGE'] = np.random.randint(18, 90, size=self.num_observations)  #TODO: to cat '18-24', '25-34', '35-44', '45-54', '55+'
        self.df['SURVEY_OPERATOR_GENDER'] = self._generate_values_with_nan(['Муж', 'Жен', "Не определен"])
    
    def generate_scores(self):
        self.df['ANS'] = np.random.randint(0, 10, self.num_observations)

        self.df['VALUE_1'] = self.df['ANS'] * np.random.uniform(-0.1, 0.05, self.num_observations)
        # self.df['VALUE_1'] = np.random.lognormal(np.log(0.6), 0.5, self.num_observations)
        self.df['VALUE_2'] = np.random.normal(loc=0.5, scale=1, size=self.num_observations)
        # self.df['VALUE_2'] =  np.random.lognormal(np.log(0.9), 0.5, self.num_observations)

        self.df['VALUE_3'] = self.df['ANS'] * np.random.uniform(0.7, 1.3, self.num_observations)
        self.df['VALUE_4'] = np.random.uniform(0.85, 1.15, self.num_observations)
    
    def generate_segments(self):
        if not self.segments or not self.segments_description:
            return
        
        for segment in self.segments:
            if segment in self.segments_description:
                values = self.segments_description[segment] # + [None]
                self.df[segment] = np.random.choice(values, self.num_observations)
            
            elif "CD" in segment and ("EVA" in segment or "FAMILY" in segment or "PRE5G" in segment or "BESPL" in segment):
                segment_prev = segment.replace("CD_", "")
                values = np.array(self.segments_description[segment_prev])

                def modify_value(x):
                    probas = np.array([0.1 / (len(values) - 1)] * len(values))
                    probas[values==x] = 0.9
                    return np.random.choice(values, p=probas)

                self.df[segment] = self.df[segment_prev].apply(modify_value)

            else:
                if 'PRODUCT' in segment or 'NETWORK' in segment:
                    self.df[segment] = np.random.choice(['Есть', 'Нет', 'Прочее', None], self.num_observations)
                elif 'PROFILE' in segment:
                    self.df[segment] = np.random.choice(['Высокий', 'Средний', 'Низкий', None], self.num_observations)
                elif 'REGION' in segment:
                    self.df[segment] = np.random.choice(['A', 'B', 'C', None], self.num_observations)
                else:
                    self.df[segment] = np.random.choice(['Вариант 1', 'Вариант 2', 'Вариант 3', None], self.num_observations)
    
    def generate_dataset(self):
        logger.info("Start Generating NPS data...")

        self.generate_ids()
        self.generate_date_columns()
        self.generate_personal_info()
        self.generate_operator_info()
        self.generate_scores()
        self.generate_segments()

        self.df.to_csv(self.out_path, index=False)

        logger.info(f"Dataset shape: {self.df.shape}")
        logger.info(f"Dataset saved in {self.out_path}")

@dataclass
class InquiryDatasetGenerator:
    data_folder: str = "data"
    num_observations: int = 1000
    min_date: str='2020-01-01'
    max_date: str='2024-12-31'
    nan_fraction: float=0.02

    def __post_init__(self):
        self.data_folder = Path(self.data_folder)
        self.in_data_folder = self.data_folder / 'raw'
        self.out_data_folder = self.data_folder / 'interim'
        self.out_path = self.out_data_folder / "inquiry_data.csv"

        with open(self.in_data_folder / 'segments', 'r', encoding='utf-8') as file:
            self.segments = [line.strip() for line in file]

        with open(self.in_data_folder / "segments_description.json", "r", encoding="utf-8") as file:
            self.segments_description = json.load(file)

        self.df = pd.DataFrame()
    
    def _generate_date(self) -> datetime:

        min_dt = datetime.strptime(self.min_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        max_dt = datetime.strptime(self.max_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")

        random_seconds = random.randint(0, int((max_dt - min_dt).total_seconds()))
        random_dt = min_dt + timedelta(seconds=random_seconds)
        
        return random_dt
    
    def _generate_values_with_nan(self, values, nan_fraction: float=None) -> np.ndarray:
        if nan_fraction is None:
            nan_fraction = self.nan_fraction
    
        p_uniform = (1 - nan_fraction) / len(values)
        probas = [p_uniform] * len(values) + [nan_fraction]
        return np.random.choice(values + [None], p=probas, size=self.num_observations)

    def generate_date_columns(self):
        self.df['SNAP_DATE'] = [self._generate_date() for _ in range(self.num_observations)]
        self.df["SNAP_WEEK"] = self.df['SNAP_DATE'].dt.isocalendar().week
        self.df["SNAP_MONTH"] = self.df['SNAP_DATE'].dt.month
        self.df["SNAP_QUARTER"] = self.df['SNAP_DATE'].dt.quarter
    
    def generate_ids(self):
        self.df['COMP_OS_ID'] = np.random.randint(1, 100, self.num_observations)
        self.df['ITPC_ITPC_ID'] = np.random.randint(1, 200, self.num_observations)
        self.df['SK_SUBS_ID'] = np.random.randint(100_000, 999_999, self.num_observations)
        self.df['SK_BMT_ID'] = np.random.randint(1, 500, self.num_observations)
        self.df['BILLING_FILIAL_ID'] = np.random.randint(1, 50, self.num_observations)
        self.df['SUBS_OS_OS_ID'] = np.random.randint(1, 90, self.num_observations)
        self.df['SUBS_SUBS_ID'] = np.random.randint(1, 90, self.num_observations)
        self.df['CR_OS_OS_ID'] = np.random.randint(1, 90, self.num_observations)
        self.df['INQR_ID'] = np.random.randint(1, 9_999_999, self.num_observations)
        self.df['FILIAL_ID'] = np.random.randint(1, 20, self.num_observations)
        self.df['CRCAT_CRCAT_ID'] = np.random.randint(1, 20, self.num_observations)
    
    def generate_geo(self):
        self.df['LAT'] = np.random.uniform(-90, 90, self.num_observations)
        self.df['LON'] = np.random.uniform(-180, 180, self.num_observations)
    
    def generate_code(self):
        self.df['CODE_LEVEL_1'] = self._generate_values_with_nan(['Сеть', 'Биллинг', 'Сервис'])
        self.df['CODE_LEVEL_2'] = self._generate_values_with_nan(['Интернет', 'Голос', 'SMS'])
        self.df['CODE_LEVEL_3'] = self._generate_values_with_nan(['Прерывания', 'Скорость', 'Ошибки'])
        self.df['CODE_LEVEL_4'] = self._generate_values_with_nan(['Нет соединения', 'Медленно', 'Ошибка тарификации'])
    
    def generate_info(self):
        self.df['DESCRIPTION'] = self._generate_values_with_nan(['Проблема с сетью', 'Ошибка в счёте', 'Некачественный сервис'])
        self.df['FULL_NAME'] = self._generate_values_with_nan(['Поломка оборудования', 'Технические работы'])
        self.df['SUBCAT_PYRAMID_NAME'] = self._generate_values_with_nan(['Высокий ARPU', 'Низкий ARPU'])
        self.df['CAT_PYRAMID_NAME'] = self._generate_values_with_nan(['VIP', 'Обычный'])
        self.df['KPI_NAME'] = self._generate_values_with_nan([
            "B2C CSI Качество Голос",
            "B2C CSI МИ",
            "B2C CSI Тарифа",
            "B2C NPS BU Качество связи",
            "B2X NPS BU Общий",
            "B2X NPS BU Общий CORE",
            "B2X NPS BU Общий F&F",
            "CSI TD Информирование",
            "CSI TD Качество голосовй связи",
            "CSI TD Качество мобильного интернета",
            "CSI TD Качество покрытия",
            "CSI TD Мобильное приложение",
            "CSI TD Обслуживание в КЦ оператора",
            "CSI TD Обслуживание в салоне",
            "CSI TD Тарифы и стоимость",
            "NPS TD",
            "NPS TD (невзвешено)"], nan_fraction=0.0
        )

        self.df['CRCAT_CRCAT_ID'] = np.random.randint(1, 20, self.num_observations)
        self.df['CR_WEIGHT'] = np.round(np.random.uniform(0.1, 5.0, self.num_observations), 2)
        self.df['FLAG_LOCAL_GENERAL'] = np.random.choice([0, 1], self.num_observations)

    def generate_dataset(self):
        logger.info("Start Generating Inquiry data...")

        self.generate_ids()
        self.generate_date_columns()
        self.generate_geo()
        self.generate_geo()
        self.generate_info()

        self.df.to_csv(self.out_path, index=False)

        logger.info(f"Dataset shape: {self.df.shape}")
        logger.info(f"Dataset saved in {self.out_path}")




