import textwrap
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List

import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import pearsonr, spearmanr

from typing import Optional
from dataclasses import dataclass
from src.utils.logging import setup_logging

import warnings
warnings.filterwarnings('ignore')

logger = setup_logging(__name__)

@dataclass
class DataVisualizer:
    nps_path: pd.DataFrame
    inquiry_path: pd.DataFrame
    segments_path: str
    output_dir: Path
    style_config: Optional[dict] = None

    def __post_init__(self):
        self.df_nps = pd.read_csv(self.nps_path)
        self.df_inquiry = pd.read_csv(self.inquiry_path)

        self.df_nps['NPS'] = np.clip(self.df_nps ['VALUE_1'] / self.df_nps ['VALUE_2'], -1, 1)

        self.nps_segments = ['SURVEY_GENDER', 'FILIAL_ID', 'SURVEY_ARPU_GROUP', 'SURVEY_CITY_SIZE', 
                'PRODUCT_TARIFF_CHANGE', 'NETWORK_PROSTOI', 'NETWORK_VOLTE', 
                'NETWORK_VOWIFI', 'NETWORK_OVERLOAD', 'NETWORK_SQI_VOICE', 
                'NETWORK_SPEED_LTE', 'NETWORK_DURATION_2G', 'NETWORK_DURATION_4G', 
                'NETWORK_SPD_LOWER_THAN_2000', 'PRODUCT_PEREPLATY', 'PROFILE_KINOPOISK',
                'PROFILE_DEVICE_TYPE', 'PROFILE_DEVICE_OS', 'PROFILE_DEVICE_PRICE',
                'PRODUCT_EVA_TYPE', 'NETWORK_SQI_DATA', 'PRODUCT_NAME_LINE_TP',
                'PRODUCT_GROUP_TARIFF', 'NETWORK_4G_SHARE', 'REGION_FILIAL']
        
        self.inquiry_segments = ['FILIAL_ID', 'DESCRIPTION', 'SUBCAT_PYRAMID_NAME', 'FULL_NAME', 'CAT_PYRAMID_NAME', 'FLAG_LOCAL_GENERAL']

        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self._setup_style()


    def _setup_style(self):
        sns.set(style="whitegrid", rc={"axes.edgecolor": "#4B0082", "axes.linewidth": 1.5})
        plt.style.use("ggplot")

        CUSTOM_PALETTE = ["#6a0dad", "#7b68ee", "#9370db", "#483d8b", "#4b0082"]
        sns.set_palette(CUSTOM_PALETTE)
        
        plt.rcParams.update({
            'figure.figsize': (14, 9),
            'figure.facecolor': 'white',
            'font.weight': 'bold',
            'axes.titleweight': 'bold',
            'axes.labelweight': 'bold',
            'axes.titlesize': 18,
            'axes.labelsize': 15,
            'axes.labelcolor': "black",  # темно-фиолетовый цвет
            'axes.edgecolor': "grey",  # Цвет рамки
            'axes.linewidth': 2,
            'xtick.labelsize': 13,
            'ytick.labelsize': 13,
            'xtick.color': "black",
            'ytick.color': "black",
            'grid.color': 'lightgray',
            'grid.alpha': 0.6,
            'grid.linestyle': '--',  # Пунктирная сетка
            'lines.linewidth': 2.5,  # Утолщенные линии графиков
            'lines.markersize': 8,  # Размер точек на графиках
            'legend.fontsize': 14,
            'legend.frameon': True,  # Включение рамки легенды
            'legend.facecolor': 'white',  # Белый фон легенды
            'legend.edgecolor': "grey",  # Цвет рамки легенды
            'font.family': 'DejaVu Sans'
        })
    
    def _save_plot(self, filename: str = "img", output_dir: Path = None) -> None:
        if output_dir is None:
            output_dir = self.output_dir
        else:
            output_dir.mkdir(exist_ok=True)
        
        path = output_dir / f"{filename}.png"
        plt.savefig(path, dpi=300, bbox_inches="tight", facecolor="white")
        logger.info(f"Image saved in path: {path}")


    def missing_values_analysis(self, df: pd.DataFrame, title: str=None, filename: str=None):
        plt.close()

        missing_percent = (df.isna().sum() / len(df)) * 100
        missing_percent = missing_percent[missing_percent > 0]
        missing_percent = missing_percent.sort_values(ascending=False)

        plt.figure(figsize=(8, 12))

        ax = sns.barplot(x=missing_percent.values, y=missing_percent.index, edgecolor="black")
        plt.tight_layout()
        plt.ylabel('')

        if title is not None:
            plt.title(title)

        self._save_plot(filename)


    def plot_segments_distribution(
            self,
            df, 
            segments: List[str], 
            title: str = "Dataset",
            filename: str = None,
            max_unique_cat: int = 15,
            figsize_width: int = 20,
            row_height: int = 5,
            wrap_width: int = 15
        ) -> None:

        plt.close()

        available_segments = [col for col in segments if col in df.columns]
        num_segments = len(available_segments)
        
        cols = min(4, num_segments)
        rows = (num_segments - 1) // cols + 1
        
        
        fig, axes = plt.subplots(
            rows, cols, 
            figsize=(figsize_width, row_height * rows),
            squeeze=False
        )

        axes = axes.flatten()

        for i, col in enumerate(available_segments):
            ax = axes[i]
            unique_vals = df[col].nunique()
            
            if pd.api.types.is_numeric_dtype(df[col]) and unique_vals > max_unique_cat:
                sns.histplot(df[col], ax=ax, kde=True, bins=30)
            else:
                if unique_vals > max_unique_cat:
                    top_vals = df[col].value_counts().nlargest(max_unique_cat).index
                    plot_data = df[df[col].isin(top_vals)]
                else:
                    plot_data = df
                
                order = plot_data[col].value_counts().index
                
                sns.countplot(
                    data=plot_data, 
                    x=col, 
                    ax=ax, 
                    order=order,
                )
                
                labels = [textwrap.fill(str(label), width=wrap_width) for label in order]
                ax.set_xticklabels(
                    labels,
                    rotation=90,
                    ha='center',
                    fontsize=9
                )
                
                ax.xaxis.set_tick_params()
            
            ax.set_title(textwrap.fill(col, width=25), fontsize=12, pad=12)
            ax.set_xlabel("")
            ax.set_ylabel("")
        
        for j in range(num_segments, len(axes)):
            fig.delaxes(axes[j])
        
        plt.tight_layout(rect=[0, 0, 1, 0.96], h_pad=3.0)
        plt.subplots_adjust(bottom=0.15)
        
        fig.suptitle(title, fontsize=16, y=0.98)
        
        self._save_plot(filename)


    def plot_top_10_filials_nps(self, df: pd.DataFrame, filename: str=None):
        plt.close()
        
        filial_nps_mean = df.groupby('REGION_FILIAL')['NPS'].mean().sort_values(ascending=False)
        top_10_filials = filial_nps_mean.head(10).index
        
        top_10_data = df[df['REGION_FILIAL'].isin(top_10_filials)]

        order = top_10_data.groupby('REGION_FILIAL')['NPS'].mean().sort_values(ascending=False).index
        
        plt.figure(figsize=(12, 8))
        
        sns.boxplot(
            data=top_10_data,
            y='REGION_FILIAL',
            x='NPS',
            order=order,
            palette="viridis",
            width=0.6,
            showfliers=False,
            meanprops={"marker":"o", "markerfacecolor":"white", "markeredgecolor":"red"}
        )
        plt.title(f'Распределение NPS в топ-10 филиалах', pad=20)
        plt.xlabel('NPS Score')
        plt.ylabel('Филиал')
        plt.grid(True, linestyle='--', alpha=0.3)
        
        plt.tight_layout()
        
        self._save_plot(filename)


    def plot_nps_by_age(self, df: pd.DataFrame, filename: str=None):
        plt.close()
        
        bins = [18, 25, 35, 45, 55, np.inf]
        labels = ['18-24', '25-34', '35-44', '45-54', '55+']
        df['AGE_GROUP'] = pd.cut(df['SURVEY_AGE'], bins=bins, labels=labels, right=False)
        
        sns.boxplot(
            data=df,
            y='NPS',
            x='AGE_GROUP',
            palette="viridis",
            showfliers=False,
        )

        plt.title(f'NPS по возрастным группам', pad=20)
        self._save_plot(filename)


    def plot_nps_by_arpu(self, df: pd.DataFrame, title="Dataset", filename: str=None):
        plt.close()
        
        groups = df['PRODUCT_REVENUE_WO_IITC_VAS_ROAM'].dropna().unique()
        colors = plt.cm.viridis(np.linspace(0, 1, len(groups)))
    
        for group, color in zip(sorted(groups), colors):
            group_data = df[df['PRODUCT_REVENUE_WO_IITC_VAS_ROAM'] == group]['NPS'].dropna()
            plt.hist(
                group_data,
                bins=30,
                alpha=0.5,
                label=group,
                color=color,
                density=True
            )
        
        plt.title(f'Распределение NPS по группам ARPU ({title})', pad=20, fontsize=14)
        plt.legend()
        plt.xlabel('NPS Score', fontsize=12)
        plt.ylabel('')

        self._save_plot(filename)
    
    def plot_geography_distribution(self, df, filename:str=None):
        plt.close()

        region_counts = df['REGION_FILIAL'].value_counts()
        
        plt.figure(figsize=(12, 8))
        sns.barplot(x=region_counts.values, y=region_counts.index)
        plt.title('География обращений клиентов')
        plt.xlabel('Количество обращений')
        plt.ylabel('Регион')
        plt.tight_layout()

        self._save_plot(filename)


    def plot_daily_dynamics_segment(self, 
                                    df: pd.DataFrame, 
                                    segment: str,
                                    metric_name, 
                                    func, 
                                    title: str,
                                    output_dir: Path,
                                    filename: str=None
                                    ):
        
        unique_groups = df[segment].dropna().unique()

        if len(unique_groups) > 10:
            pass

        fig, axs = plt.subplots(len(unique_groups), 1, figsize=(20, len(unique_groups) * 6))

        for idx in range(len(unique_groups)):
            group = unique_groups[idx]
            ax = axs[idx]

            df_group = df[df[segment] == group]

            df_group['SNAP_DATE_DAY'] = pd.to_datetime(df_group['SNAP_DATE']).dt.to_period('D')
        
            daily_counts = df_group.groupby('SNAP_DATE_DAY').agg(
                METRIC=(metric_name, func)
            ).reset_index()

            daily_counts['SNAP_DATE_DAY'] = daily_counts['SNAP_DATE_DAY'].dt.to_timestamp()

            sns.lineplot(
                x='SNAP_DATE_DAY',
                y='METRIC',
                data=daily_counts,
                ax=ax,
                color='#9370DB',
                linewidth=2.5,
                marker='o',
                markersize=8,
                markeredgecolor='white',
                markeredgewidth=1
            )

            ax.set_title(f'{group}', fontsize=14)
            ax.set_ylabel(metric_name, fontsize=12)
            ax.set_xlabel('')
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        
        axs[-1].set_xlabel('Time')
        
        fig.suptitle(f'{title} in Segment: {segment}')

        plt.tight_layout()
        self._save_plot(output_dir=output_dir, filename=filename)


    def plot_daily_dynamics(self, df: pd.DataFrame, metric_name, func, title: str='', filename: str=None):
        plt.close()
        fig, ax1 = plt.subplots(1, 1, figsize=(24, 6))
        
        df = df.copy()

        df['SNAP_DATE_DAY'] = pd.to_datetime(df['SNAP_DATE']).dt.to_period('D')
        
        daily_counts = df.groupby('SNAP_DATE_DAY').agg(
            METRIC=(metric_name, func)
        ).reset_index()

        daily_counts['SNAP_DATE_DAY'] = daily_counts['SNAP_DATE_DAY'].dt.to_timestamp()

        sns.lineplot(
            x='SNAP_DATE_DAY',
            y='METRIC',
            data=daily_counts,
            ax=ax1,
            color='#9370DB',
            linewidth=2.5,
            marker='o',
            markersize=8,
            markeredgecolor='white',
            markeredgewidth=1
        )
        
        ax1.set_title(title, fontsize=14)
        ax1.set_ylabel(metric_name, fontsize=12)
        ax1.set_xlabel('')

        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        
        plt.tight_layout()
        
        self._save_plot(filename)


    def plot_correlation_daily_metrics(
            self,
            df1: pd.DataFrame, df2: pd.DataFrame,
            group_col1: str, group_col2: str,
            func1: str, func2: str,
            title: str='',
            metric1_name: str='', metric2_name: str='',
            filename: str=None
        ):
        plt.close()
        fig, ax1 = plt.subplots(1, 1, figsize=(10, 8))
        
        df1 = df1.copy()
        df2 = df2.copy()
        
        df1['SNAP_DATE_DAY'] = pd.to_datetime(df1['SNAP_DATE']).dt.to_period('D')
        df2['SNAP_DATE_DAY'] = pd.to_datetime(df2['SNAP_DATE']).dt.to_period('D')
        
        daily_counts_1 = df1.groupby('SNAP_DATE_DAY').agg(
            METRIC_1=(group_col1, func1)
        ).reset_index()
        
        daily_counts_2 = df2.groupby('SNAP_DATE_DAY').agg(
            METRIC_2=(group_col2, func2)
        ).reset_index()
        
        daily_counts_1['SNAP_DATE_DAY'] = daily_counts_1['SNAP_DATE_DAY'].dt.to_timestamp()
        daily_counts_2['SNAP_DATE_DAY'] = daily_counts_2['SNAP_DATE_DAY'].dt.to_timestamp()
        
        daily_counts_merged = pd.merge(daily_counts_1, daily_counts_2, on='SNAP_DATE_DAY')
        
        pearson_corr, pearson_p = pearsonr(daily_counts_merged['METRIC_1'], daily_counts_merged['METRIC_2'])
        spearman_corr, spearman_p = spearmanr(daily_counts_merged['METRIC_1'], daily_counts_merged['METRIC_2'])
        
        sns.scatterplot(
            x='METRIC_1',
            y='METRIC_2',
            data=daily_counts_merged,
            ax=ax1,
            color='skyblue',
            edgecolor='k'
        )
        
        ax1.set_title(title, fontsize=14)
        ax1.set_xlabel(metric1_name, fontsize=12)
        ax1.set_ylabel(metric2_name, fontsize=12)
        
        legend_text = (f'Pearson corr: {pearson_corr:.2f} (p={pearson_p:.3g})\n'
                    f'Spearman corr: {spearman_corr:.2f} (p={spearman_p:.3g})')
        ax1.text(0.05, 0.95, legend_text, transform=ax1.transAxes,
                fontsize=12, verticalalignment='top', bbox=dict(facecolor='white', alpha=0.8))
        
        plt.tight_layout()

        self._save_plot(filename)


    def generate_images(self):
        self.missing_values_analysis(self.df_nps, title='Missing values distribution of NPS dataset', filename='missing_vals_nps')
        self.missing_values_analysis(self.df_inquiry, title='Missing values distribution of Inquiry dataset', filename='missing_vals_inquiry')

        self.plot_segments_distribution(self.df_nps, segments=self.nps_segments, title="Segments Ditribution in NPS dataset", filename="segments_dist_nps")

        self.plot_top_10_filials_nps(self.df_nps, filename="nps_in_top10_filials")
        self.plot_nps_by_age(self.df_nps, filename="nps_dist_by_age")

        self.plot_nps_by_arpu(self.df_nps, filename='nps_dist_arpu')

        self.plot_geography_distribution(self.df_nps, filename='geography_query_distribution')

        self.plot_daily_dynamics(
            df=self.df_nps, 
            metric_name='NPS',
            func='mean',
            title='Динамика среднего NPS по дням',
            filename="timeline_nps"
        )

        self.plot_daily_dynamics(
            df=self.df_inquiry, 
            metric_name='FILIAL_ID',  # doesn't matter which column to pass
            func='size',
            title='Динамика обращений по дням',
            filename="timeline_inquiry"
        )

        for segment in self.nps_segments:
            self.plot_daily_dynamics_segment(
                df=self.df_nps,
                segment=segment,
                metric_name='NPS', 
                func='mean',
                title="Daily NPS dynamic",
                output_dir=self.output_dir / "segments_nps_time",
                filename=f"timeline_nps_{segment}"
                )
        
        for segment in self.inquiry_segments:
            self.plot_daily_dynamics_segment(
                df=self.df_inquiry, 
                segment=segment, 
                metric_name='FILIAL_ID', 
                func='size',
                title="Daily Inquiry dynamic",
                output_dir=self.output_dir / "segments_inquiry_time",
                filename=f"timeline_inquiry_{segment}"
                )
        
        self.plot_correlation_daily_metrics(
            df1=self.df_nps, df2=self.df_inquiry,
            group_col1='NPS', group_col2='FILIAL_ID',
            func1='mean', func2='size',
            metric1_name='MEAN NPS', metric2_name='TOTAL INQUIRY',
            title='Inquiry and NPS dependence',
            filename=f'nps_inquiry_correlation'
        )