import argparse
from src.utils.config_parser import load_config
from src.utils.logging import setup_logging
from src.pipeline.eda import DataVisualizer
from src.pipeline.dataset_generator import NPSDatasetGenerator, InquiryDatasetGenerator
from src.pipeline.preprocessing import NPSDataProcessor, InquiryDataProcessor
from src.pipeline.anomalies_detector import AnomalyDetector

logger = setup_logging(__name__)

def main():
    parser = argparse.ArgumentParser(description='NPS Dataset Generator')
    parser.add_argument('--config_path', type=str, required=True, help='Path to config file')
    args = parser.parse_args()

    config = load_config(args.config_path)
    nps_generator_params = config.get("NPSDatasetGenerator", {})
    inquiry_generator_params = config.get("InquiryDatasetGenerator", {})
    datavis_params = config.get("DataVisualizer", {})
    processor_params = config.get("DataProcessor", {})
    detector_params = config.get("AnomalyDetector", {})

    nps_generator = NPSDatasetGenerator(**nps_generator_params)
    inquiry_generator = InquiryDatasetGenerator(**inquiry_generator_params)

    # nps_generator.generate_dataset()
    # inquiry_generator.generate_dataset()

    data_visualizer = DataVisualizer(nps_path=nps_generator.out_path,
                                     inquiry_path=inquiry_generator.out_path,
                                     segments_path=f"{nps_generator_params['data_folder']}/raw/segments",
                                     **datavis_params)

    data_visualizer.generate_images()

    nps_processor = NPSDataProcessor(data_path=nps_generator.out_path, output_name="nps_processed", **processor_params)
    inquiry_processor = InquiryDataProcessor(data_path=inquiry_generator.out_path, output_name="inquiry_processed", **processor_params)

    nps_processor.process()
    inquiry_processor.process()

    nps_detector = AnomalyDetector(metric_name="NPS", func='mean', **detector_params)
    inquiry_detector = AnomalyDetector(metric_name="FILIAL_ID", func='size', **detector_params)  # doesn't matter whih column to pass with size

    nps_detector.process_segments(path_to_df=nps_processor.output_file, output_name="nps_significant_segments")
    inquiry_detector.process_segments(path_to_df=inquiry_processor.output_file, output_name="inquiry_significant_segments")


if __name__ == "__main__":
    main()