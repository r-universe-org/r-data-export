import fs from 'node:fs';

/**
 * Keeps track of failed requests: the most recent ones in memory (see summary()),
 * and optionally all of them as json lines in a file, which survives restarts.
 * The file is rotated to file.1 when it exceeds maxFileBytes.
 */
export function createErrorLog({ file, maxEntries = 1000, maxFileBytes = 50e6 } = {}) {
  const started = new Date().toISOString();
  const recent = [];
  const counts = {};
  let fileSize = 0;
  if (file) {
    try {
      fileSize = fs.statSync(file).size;
    } catch (err) {}
  }

  function write(entry) {
    const line = JSON.stringify(entry) + '\n';
    if (fileSize + line.length > maxFileBytes) {
      fs.renameSync(file, `${file}.1`);
      fileSize = 0;
    }
    fs.appendFileSync(file, line);
    fileSize += line.length;
  }

  function add(entry) {
    console.log(`[R-DATA-EXPORT] ${entry.status} ${entry.universe}/${entry.package}::${entry.dataset} (${entry.format}): ${entry.message}`);
    const key = `${entry.stage} ${entry.status}`;
    counts[key] = (counts[key] || 0) + 1;
    recent.push(entry);
    if (recent.length > maxEntries) recent.shift();
    if (file) {
      try {
        write(entry);
      } catch (err) {
        console.log(`[R-DATA-EXPORT] Failed to write error log: ${err.message}`);
      }
    }
  }

  // Export errors are the interesting ones: those passed the metadata checks but failed anyway
  function summary(stage) {
    const entries = stage ? recent.filter(x => x.stage === stage) : recent;
    return { since: started, counts, recent: entries.slice().reverse() };
  }

  return { add, summary };
}
