# The old seed pipeline
import logging
import emission.analysis.classification.inference.mode.seed.pipeline as pipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    seed_pipeline = pipeline.ModeInferencePipelineMovesFormat()
    seed_pipeline.runPipeline()
