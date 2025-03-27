import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass

from sklearn.impute import KNNImputer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import LabelEncoder

from src.utils.logging import setup_logging

logger = setup_logging(__name__)

np.random.seed(42)

@dataclass
class BaseDataProcessor:
    data_path: str=None
    output_name: str=None
    output_dir: str="data"
    required_cols: set=None
    data: pd.DataFrame=None

    def __post_init__(self):
        if self.data is None and self.data_path is not None:
            self.data = pd.read_csv(self.data_path)

        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.output_file = self.output_dir / f"{self.output_name}_data_coded.csv"

    def process_date(self):
        if 'SNAP_DATE' in self.data.columns:
            self.data['SNAP_DATE'] = pd.to_datetime(self.data['SNAP_DATE'])
            self.data['SNAP_DATE_DAY'] = self.data['SNAP_DATE'].dt.strftime('%Y-%m-%d')
            self.required_cols.add('SNAP_DATE_DAY')
    
    def encode_filial(self):
        if 'FILIAL_ID' in self.data.columns:
            self.data['FILIAL_ID'] = self.data['FILIAL_ID'].astype('object')

    def drop_cols(self):
        self.data = self.data.loc[:, list(self.required_cols)]

    def fill_missing_values(self):
        logger.info(f"{self.__class__.__name__}: Filling missing values using ML models...")
        logger.info(f"Initial missing values distribution: \n{self.data.isna().sum()}")

        num_cols = self.data.select_dtypes(include=['number']).columns.tolist()
        cat_cols = self.data.select_dtypes(include=['object']).columns.tolist()

        # KNN для числовых данных
        if num_cols:
            knn_imputer = KNNImputer(n_neighbors=5)
            self.data[num_cols] = knn_imputer.fit_transform(self.data[num_cols])

        # RandomForest для категориальных данных
        label_encoders = {}
        for col in cat_cols:
            le = LabelEncoder()
            self.data[col] = self.data[col].astype(str).fillna("missing")
            self.data[col] = le.fit_transform(self.data[col])
            label_encoders[col] = le

        for col in cat_cols:
            if self.data[col].isnull().sum() > 0:
                self._fill_categorical_missing(col)
        
        for col, le in label_encoders.items():
            self.data[col] = le.inverse_transform(self.data[col].astype(int))
        
        logger.info(f"Missing values distribution after imputation: \n{self.data.isna().sum()}")

    def _fill_categorical_missing(self, col):
        """Заполняем пропуски в категориальном признаке через RandomForest"""
        df = self.data.copy()
        missing_mask = df[col].isnull()

        # Кодируем категории в числа
        le = LabelEncoder()
        df[col] = df[col].astype(str).apply(lambda x: np.nan if x == 'nan' else x)

        known_data = df.loc[~missing_mask, :]
        unknown_data = df.loc[missing_mask, :]

        if unknown_data.empty:
            return
        
        # Обучаем модель
        le.fit(known_data[col])
        known_data[col] = le.transform(known_data[col].values)

        features = known_data.drop(columns=[col])
        target = known_data[col].values

        print(target)

        model = (RandomForestRegressor(n_estimators=100, random_state=42) 
                 if np.unique(target).shape[0] > 5 
                 else RandomForestClassifier(n_estimators=100, random_state=42))
        model.fit(features, target)

        # Предсказание
        predicted_values = model.predict(unknown_data.drop(columns=[col]))
        df.loc[missing_mask, col] = predicted_values
        df[col] = le.inverse_transform(df[col].astype(int))

        self.data[col] = df[col]

    def process(self):
        logger.info(f"{self.__class__.__name__}: Initial dataset shape: {self.data.shape}")

        self.process_date()
        self.drop_cols()
        self.fill_missing_values()

        logger.info(f"{self.__class__.__name__}: Processed dataset shape: {self.data.shape}")

        self.data.to_csv(self.output_file, index=False)
        logger.info(f"{self.__class__.__name__}: Data saved to {self.output_file}")


@dataclass
class NPSDataProcessor(BaseDataProcessor):
    required_cols: set = None

    def __post_init__(self):
        self.required_cols = {
            'SURVEY_GENDER', 'FILIAL_ID', 'SURVEY_ARPU_GROUP', 'SURVEY_CITY_SIZE',
            'PRODUCT_TARIFF_CHANGE', 'NETWORK_PROSTOI', 'NETWORK_VOLTE',
            'NETWORK_VOWIFI', 'NETWORK_OVERLOAD', 'NETWORK_SQI_VOICE',
            'NETWORK_SPEED_LTE', 'NETWORK_DURATION_2G', 'NETWORK_DURATION_4G',
            'NETWORK_SPD_LOWER_THAN_2000', 'PRODUCT_PEREPLATY', 'PROFILE_KINOPOISK',
            'PROFILE_DEVICE_TYPE', 'PROFILE_DEVICE_OS', 'PROFILE_DEVICE_PRICE',
            'PRODUCT_EVA_TYPE', 'NETWORK_SQI_DATA', 'PRODUCT_NAME_LINE_TP',
            'PRODUCT_GROUP_TARIFF', 'NETWORK_4G_SHARE', 'REGION_FILIAL'
        }
        super().__post_init__()

    def _encode_age(self, x):
        if 18 <= x < 25:
            return '18-24'
        elif 25 <= x < 35:
            return '25-34'
        elif 25 <= x < 45:
            return '35-44'
        elif 45 <= x < 55:
            return '45-54'
        elif x > 55:
            return '55+'
        return x

    def calculate_nps(self):
        self.data['NPS'] = np.clip(self.data['VALUE_1'] / self.data['VALUE_2'], -1, 1)
        self.required_cols.add('NPS')

    def code_age_group(self):
        self.data['SURVEY_AGE_GROUP'] = self.data['SURVEY_AGE'].apply(self._encode_age)
        self.data['SURVEY_OPERATOR_AGE_GROUP'] = self.data['SURVEY_OPERATOR_AGE'].apply(self._encode_age)
        self.required_cols.update({'SURVEY_AGE_GROUP', 'SURVEY_OPERATOR_AGE_GROUP'})

    def process(self):
        logger.info(f"{self.__class__.__name__}: Initial dataset shape: {self.data.shape}")

        self.calculate_nps()
        self.code_age_group()
        super().process()


@dataclass
class InquiryDataProcessor(BaseDataProcessor):
    required_cols: set = None

    def __post_init__(self):
        self.required_cols = {
            'FILIAL_ID', 'DESCRIPTION', 'SUBCAT_PYRAMID_NAME',
            'FULL_NAME', 'CAT_PYRAMID_NAME', 'FLAG_LOCAL_GENERAL'
        }
        super().__post_init__()
