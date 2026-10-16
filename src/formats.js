// Supported output formats and how they are served
export const formats = {
  'csv': { type: 'text/csv', filename: '.csv' },
  'csv.gz': { type: 'application/gzip', filename: '.csv.gz' },
  'xlsx': { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename: '.xlsx' },
  'json': { type: 'application/json' },
  'ndjson': { type: 'text/plain' },
  'rds': { type: 'application/octet-stream', filename: '.rds' },
  'rda': { type: 'application/octet-stream', filename: '.RData' }
};
