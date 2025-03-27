from dataclasses import dataclass
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from tqdm import tqdm
from pathlib import Path
from src.utils.logging import setup_logging

logger = setup_logging(__name__)

@dataclass
class AnomalyDetector:
    metric_name: str = 'NPS'
    func: str = 'mean'
    window_size: int = 7
    z_threshold: float = 3.0
    ml_contamination: float = 0.05
    min_group_size: int = 2
    clip_contributions: bool = True
    output_dir: str = None
    
    def __post_init__(self):
        self.required_columns = ['SNAP_DATE_DAY', self.metric_name]
    
        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def _prepare_base_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare base dataframe with overall metrics"""
        agg_df = df.groupby('SNAP_DATE_DAY')[self.metric_name].agg([self.func, 'size'])
        agg_df.columns = [self.metric_name, 'total_count']
        agg_df[f'delta_{self.metric_name}_all'] = agg_df[self.metric_name].diff().bfill()
        return agg_df.reset_index()

    def _process_single_group(self, group_df: pd.DataFrame) -> pd.DataFrame:
        """Process data for a single segment group"""
        result = group_df.groupby('SNAP_DATE_DAY')[self.metric_name].agg([self.func, 'count'])
        result.columns = [self.metric_name, 'count']
        result[f'delta_{self.metric_name}'] = result[self.metric_name].diff().bfill()
        
        # Anomaly detection
        model = IsolationForest(
            contamination=self.ml_contamination, 
            random_state=42
        )
        result['ml_anomaly'] = model.fit_predict(result[[f'delta_{self.metric_name}']]) == -1
        
        # Z-score calculation
        rolling = result[self.metric_name].rolling(
            window=self.window_size, 
            min_periods=1
        )
        result['z_score'] = (result[f'delta_{self.metric_name}'] - rolling.mean()) / (rolling.std() + 1e-6)
        
        return result.reset_index()

    def _calculate_contribution(self, base_df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        """Calculate contribution for a specific group"""
        delta_total_col = f'delta_{self.metric_name}_all'
        delta_segment_col = f'delta_{self.metric_name}_{suffix}'
        count_col = f'count_{suffix}'
        
        # Calculate contribution
        weight = base_df[count_col] / base_df['total_count']
        delta_segment = base_df[delta_segment_col]
        delta_total = base_df[delta_total_col]
        
        contribution = (delta_segment * weight) / delta_total
        contribution = contribution.replace([np.inf, -np.inf], 0)
        
        if self.clip_contributions:
            contribution = contribution.clip(-1, 1)
            
        base_df[f'contribution_{suffix}'] = np.where(
            np.isclose(delta_total, 0), 
            0, 
            contribution
        )
        return base_df

    def _merge_group_data(self, base_df: pd.DataFrame, processed: pd.DataFrame, suffix: str) -> pd.DataFrame:
        """Merge processed group data with base dataframe"""
        merge_cols = ['SNAP_DATE_DAY', 
                    f'delta_{self.metric_name}',
                    'ml_anomaly', 
                    'z_score', 
                    'count']
        
        return base_df.merge(
            processed[merge_cols],
            on='SNAP_DATE_DAY',
            how='left',
            suffixes=('', f'_{suffix}')
        ).rename(columns={
            f'delta_{self.metric_name}': f'delta_{self.metric_name}_{suffix}',
            'ml_anomaly': f'ml_anomaly_{suffix}',
            'z_score': f'z_score_{suffix}',
            'count': f'count_{suffix}'
        })

    def _detect_significant_anomalies(self, base_df: pd.DataFrame, segment_name: str):
        """Detect significant anomalies for ML and Z-score"""
        ml_cols = [c for c in base_df.columns if f'ml_anomaly_{segment_name}_' in c]
        z_cols = [c for c in base_df.columns if f'z_score_{segment_name}_' in c]
        
        # ML anomalies
        base_df[f'ml_anomaly_{segment_name}_significant'] = base_df[ml_cols].apply(
            lambda row: self._find_max_contributor(row, segment_name), 
            axis=1
        )
        
        # Z-score anomalies
        z_anomalies_mask = base_df[z_cols].abs() > self.z_threshold
        base_df[f'z_anomaly_{segment_name}_significant'] = z_anomalies_mask.apply(
            lambda row: self._find_max_contributor(row, segment_name), 
            axis=1
        )

        condition = (
            (base_df[f'ml_anomaly_{segment_name}_significant'] == 
            base_df[f'z_anomaly_{segment_name}_significant']) &
            (base_df[f'ml_anomaly_{segment_name}_significant'] != False)
        )
    
        base_df[f'anomaly_{segment_name}_significant'] = np.where(
            condition,
            base_df[f'ml_anomaly_{segment_name}_significant'],
            False
        )

        return base_df

    def _find_max_contributor(self, row: pd.Series, segment_name: str):
        """Find group with maximum absolute contribution"""
        max_contrib = -1
        significant_group = None
        
        for col in row.index:
            if row[col]:
                group = col.split('_')[-1]
                contrib_col = f'contribution_{segment_name}_{group}'
                contrib_value = abs(row[contrib_col]) if contrib_col in row else 0
                
                if contrib_value > max_contrib:
                    max_contrib = contrib_value
                    significant_group = group
                    
        return significant_group if significant_group else False

    def process_segments(self, df: pd.DataFrame=None, path_to_df: str=None, output_name: str=None) -> pd.DataFrame:
        """Main processing pipeline"""
        if df is None and path_to_df is not None:
            df = pd.read_csv(path_to_df)

        segments = sorted(list(set(df.columns) - set(['SNAP_DATE_DAY', self.metric_name])))

        base_df = self._prepare_base_data(df)

        min_date = df['SNAP_DATE_DAY'].min()
        max_date = df['SNAP_DATE_DAY'].max()
        logger.info(f"Processing data from {min_date} to {max_date}")
        
        for segment in tqdm(segments, colour='GREEN'):
            unique_groups = df[segment].dropna().unique()

            logger.info(f"Processing segment: {segment}")
            logger.info(f"Total groups in segment: {len(unique_groups)}")
            
            for group in unique_groups:
                group_df = df[df[segment] == group]
                if len(group_df) < self.min_group_size:
                    logger.debug(f"Skipping group {group} (size: {len(group_df)})")
                    continue
                    
                processed = self._process_single_group(group_df)
                suffix = f'{segment}_{group}'
                
                # Merge and rename columns
                base_df = self._merge_group_data(base_df, processed, suffix)
                
                # Calculate contribution
                base_df = self._calculate_contribution(base_df, suffix)
            
            # Detect significant anomalies
            base_df = self._detect_significant_anomalies(base_df, segment)
            
            # Cleanup columns
            cols_to_remove = [c for c in base_df.columns 
                        if any(c.endswith(f'_{segment}_{gr}') for gr in unique_groups)
                        and not (c.startswith('contribution_') or c.startswith('z_score_') or c.startswith('anomaly_'))]
            base_df = base_df.drop(columns=cols_to_remove)
            
        
        # 1. Create and save z-scores table
        z_columns = [col for col in base_df.columns if col.startswith('z_score_')]
        z_scores_df = base_df[['SNAP_DATE_DAY'] + z_columns].copy()
        z_scores_df.to_csv(
            self.output_dir / f"{output_name}_z_scores.csv", 
            index=False
        )

        # 2. Create and save contribution table
        contribution_columns = [col for col in base_df.columns if col.startswith('contribution_')]
        contribution_df = base_df[['SNAP_DATE_DAY'] + contribution_columns].copy()
        contribution_df.to_csv(
            self.output_dir / f"{output_name}_contribution.csv", 
            index=False
        )

        if output_name is not None:
            base_df = base_df.drop(columns=contribution_columns + z_columns)
            base_df.to_csv(
                self.output_dir / f"{output_name}.csv", 
                index=False
            )

            logger.info(f"Processing completed. Results saved to: {self.output_dir}")

        return base_df
