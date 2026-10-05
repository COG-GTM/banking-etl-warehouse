CREATE VOLUME IF NOT EXISTS ${landing_volume}
  COMMENT 'Landing zone: transactions/{csv,excel}/ and sample_db/<table>/ (fixture mode).';
CREATE VOLUME IF NOT EXISTS ${checkpoint_volume}
  COMMENT 'Auto Loader / Structured Streaming checkpoints and schema locations.';
