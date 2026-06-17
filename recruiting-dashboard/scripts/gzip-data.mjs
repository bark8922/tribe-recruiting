// Build step: gzip the large Snowflake data file into public/ so it ships as a
// fetched asset instead of being inlined into the JS bundle. Inlining it pushed
// the bundle past Cloudflare Pages' 25 MiB per-file limit and broke every deploy
// from 2026-06-15 onward. Gzipping (~10x) keeps us well under the ceiling and
// lets the data grow a lot before this is a problem again.
import { gzipSync } from 'node:zlib';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';

const SRC = 'src/dashboard_data_snowflake.json';
const OUT = 'public/dashboard_data_snowflake.json.gz';
const MiB = 1048576;

if (!existsSync('public')) mkdirSync('public', { recursive: true });
const raw = readFileSync(SRC);
const gz = gzipSync(raw, { level: 9 });
writeFileSync(OUT, gz);
console.log(`gzip-data: ${SRC} ${(raw.length / MiB).toFixed(1)} MiB -> ${OUT} ${(gz.length / MiB).toFixed(2)} MiB`);
if (gz.length > 24 * MiB) {
  console.error(`gzip-data: WARNING gz asset is ${(gz.length / MiB).toFixed(2)} MiB, approaching the 25 MiB Cloudflare Pages limit. Time to split the data file.`);
}
