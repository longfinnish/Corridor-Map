const https = require('https');
const fs = require('fs');
const path = require('path');

const DATA = path.join(__dirname, 'data');
const LOCS_DIR = path.join(DATA, 'operator_locations');
if (!fs.existsSync(LOCS_DIR)) fs.mkdirSync(LOCS_DIR, { recursive: true });

function fetchUrl(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const options = {
      timeout: 60000,
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', ...opts.headers },
      method: opts.method || 'GET',
    };
    const req = https.request(url, options, res => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchUrl(res.headers.location, opts).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf-8');
        resolve({ status: res.statusCode, body, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    if (opts.body) req.write(opts.body);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Parse CSV into array of objects (handles quoted fields with commas)
function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];

  function splitCSVLine(line) {
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === ',' && !inQuotes) {
        fields.push(current.trim());
        current = '';
      } else {
        current += ch;
      }
    }
    fields.push(current.trim());
    return fields;
  }

  const headers = splitCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = splitCSVLine(lines[i]);
    const obj = {};
    headers.forEach((h, j) => { obj[h] = vals[j] || ''; });
    rows.push(obj);
  }
  return rows;
}

const allLocations = []; // Normalized output

async function pullET() {
  const assets = [
    { code: 'PEPL', name: 'Panhandle Eastern' },
    { code: 'TGC', name: 'Trunkline' },
    { code: 'FGT', name: 'Florida Gas' },
    { code: 'TW', name: 'Transwestern' },
    { code: 'TGR', name: 'Tiger' },
    { code: 'SPC', name: 'Sea Robin' },
    { code: 'ROVER', name: 'Rover' },
    { code: 'MRT', name: 'Enable MRT' },
    { code: 'EGT', name: 'CenterPoint EGT' },
  ];

  console.log('=== Energy Transfer ===');
  for (const a of assets) {
    try {
      const url = `https://pipelines.energytransfer.com/ipost/downloads/measuring-point?asset=${a.code}`;
      const r = await fetchUrl(url);
      if (r.status === 200 && r.body.length > 100) {
        fs.writeFileSync(path.join(LOCS_DIR, `et_${a.code.toLowerCase()}_locations.csv`), r.body);
        const rows = parseCSV(r.body);
        console.log(`  ${a.code} (${a.name}): ${rows.length} locations`);

        rows.forEach(row => {
          allLocations.push({
            pipeline: a.name,
            pipeline_code: a.code,
            source: 'energy_transfer',
            point_id: row['Loc'] || row['LOC'] || row['LOCATION'] || row['Loc Prop'] || '',
            point_name: row['Loc Name'] || row['LOC NAME'] || row['LOCATION NAME'] || row['Loc Nm'] || '',
            county: row['Loc Cnty'] || row['LOC CNTY ABBREV'] || row['COUNTY'] || row['Loc County'] || '',
            state: row['Loc St Abbrev'] || row['LOC ST ABBREV'] || row['STATE'] || row['Loc St'] || row['State'] || '',
            type: row['Loc Type Ind'] || row['LOC TYPE IND'] || row['LOCATION TYPE'] || '',
            flow: row['Dir Flo'] || row['DIR FLO'] || '',
            connected: row['Up/Dn Name'] || row['UP/DN NAME'] || row['LOCATION OPERATOR'] || '',
          });
        });
      } else {
        console.log(`  ${a.code}: HTTP ${r.status}, ${r.body.length} bytes`);
      }
    } catch(e) {
      console.log(`  ${a.code}: ERROR ${e.message}`);
    }
    await sleep(1000);
  }
}

async function pullNNG() {
  console.log('\n=== Northern Natural Gas ===');
  try {
    const url = 'https://www.northernnaturalgas.com/infopostings/Pages/Locations.aspx?download=true';
    const r = await fetchUrl(url);
    if (r.status === 200 && r.body.length > 100) {
      fs.writeFileSync(path.join(LOCS_DIR, 'nng_locations.csv'), r.body);
      const rows = parseCSV(r.body);
      console.log(`  NNG: ${rows.length} locations`);

      rows.forEach(row => {
        allLocations.push({
          pipeline: 'Northern Natural',
          pipeline_code: 'NNG',
          source: 'northern_natural',
          point_id: row['Loc'] || '',
          point_name: row['Loc Name'] || '',
          county: row['Loc Cnty'] || '',
          state: row['Loc St Abbrev'] || '',
          type: row['Loc Type Ind'] || '',
          flow: row['Dir Flo'] || '',
          connected: row['Up/Dn Name'] || '',
        });
      });
    }
  } catch(e) {
    console.log(`  NNG: ERROR ${e.message}`);
  }
}

async function pullKM(code, name) {
  try {
    // Step 1: GET the page to get VIEWSTATE
    const pageUrl = `https://pipeline2.kindermorgan.com/LocationDataDownload/LocDataDwnld.aspx?code=${code}`;
    const r1 = await fetchUrl(pageUrl);
    if (r1.status !== 200) {
      console.log(`  ${code}: HTTP ${r1.status} on page load`);
      return;
    }

    // Extract ALL hidden form fields (ASP.NET needs them all)
    const hidden = {};
    const re1 = /<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"/g;
    let m;
    while ((m = re1.exec(r1.body)) !== null) hidden[m[1]] = m[2];
    const re2 = /<input[^>]*value="([^"]*)"[^>]*type="hidden"[^>]*name="([^"]+)"/g;
    while ((m = re2.exec(r1.body)) !== null) hidden[m[2]] = m[1];

    if (!hidden['__VIEWSTATE']) {
      console.log(`  ${code}: no VIEWSTATE found`);
      return;
    }

    // Step 2: POST to trigger CSV download
    const formData = new URLSearchParams(hidden);
    formData.set('ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL', 'CSV');
    formData.set('ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x', '15');
    formData.set('ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y', '15');
    formData.set('ctl00$hdnIsDownload', 'true');

    // Get cookies from first request
    const rawCookies = r1.headers['set-cookie'] || [];
    const cookies = (Array.isArray(rawCookies) ? rawCookies : [rawCookies])
      .map(c => c.split(';')[0]).join('; ');

    const r2 = await fetchUrl(pageUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookies,
        'Referer': pageUrl,
      },
      body: formData.toString(),
    });

    const isCSV = !r2.body.trim().startsWith('<!DOCTYPE') && !r2.body.trim().startsWith('<html') &&
                  (r2.body.includes('LOC') || r2.body.includes('Loc'));
    if (r2.status === 200 && r2.body.length > 100 && isCSV) {
      fs.writeFileSync(path.join(LOCS_DIR, `km_${code.toLowerCase()}_locations.csv`), r2.body);
      const rows = parseCSV(r2.body);
      console.log(`  ${code} (${name}): ${rows.length} locations`);

      rows.forEach(row => {
        allLocations.push({
          pipeline: name,
          pipeline_code: code,
          source: 'kinder_morgan',
          point_id: row['LOC'] || row['Loc'] || '',
          point_name: row['LOC NAME'] || row['Loc Name'] || '',
          county: row['LOC CNTY ABBREV'] || row['Loc Cnty'] || row['LOC COUNTY'] || '',
          state: row['LOC ST ABBREV'] || row['Loc St Abbrev'] || row['STATE'] || '',
          type: row['LOC TYPE IND'] || row['Loc Type Ind'] || row['LOC TYPE'] || '',
          flow: row['DIR FLO'] || row['Dir Flo'] || '',
          connected: row['UP/DN NAME'] || row['Up/Dn Name'] || '',
        });
      });
    } else {
      console.log(`  ${code}: POST returned ${r2.status}, ${r2.body.length} bytes (not CSV)`);
    }
  } catch(e) {
    console.log(`  ${code}: ERROR ${e.message}`);
  }
}

async function pullAllKM() {
  console.log('\n=== Kinder Morgan ===');
  const pipelines = [
    { code: 'TGP', name: 'Tennessee Gas' },
    { code: 'NGPL', name: 'NGPL' },
    { code: 'EPNG', name: 'El Paso' },
    { code: 'SNG', name: 'Southern Natural' },
    { code: 'CIG', name: 'Colorado Interstate' },
    { code: 'MEP', name: 'Midcontinent Express' },
    { code: 'KMLP', name: 'KM Louisiana' },
    { code: 'KMIL', name: 'KM Illinois' },
    { code: 'WIC', name: 'WIC' },
    { code: 'TCP', name: 'TransColorado' },
    { code: 'CP', name: 'Cheyenne Plains' },
    { code: 'MOPC', name: 'Mojave' },
    { code: 'SGP', name: 'Sierrita' },
    { code: 'EEC', name: 'Elba Express' },
    { code: 'STAG', name: 'Stagecoach' },
    { code: 'ARLS', name: 'Arlington Storage' },
    { code: 'YGS', name: 'Young Gas Storage' },
    // Intrastate
    { code: 'KMTP', name: 'KM Texas' },
    { code: 'KMTJ', name: 'KM Tejas' },
    { code: 'KMNT', name: 'KM North Texas' },
    { code: 'GCX', name: 'GCX' },
    { code: 'PHP', name: 'PHP' },
    { code: 'KMBP', name: 'KM Border' },
    { code: 'NETM', name: 'NET Mexico' },
    { code: 'KMEF', name: 'Eagle Ford' },
  ];

  for (const pl of pipelines) {
    await pullKM(pl.code, pl.name);
    await sleep(2000); // Be polite to KM
  }
}

async function main() {
  await pullET();
  await pullNNG();
  await pullAllKM();

  // Save normalized locations
  fs.writeFileSync(path.join(LOCS_DIR, '_all_operator_locations.json'), JSON.stringify(allLocations, null, 2) + '\n');
  console.log(`\n=== Total: ${allLocations.length} operator location records ===`);

  // Summary by pipeline
  const byPipeline = {};
  allLocations.forEach(l => {
    if (!byPipeline[l.pipeline]) byPipeline[l.pipeline] = 0;
    byPipeline[l.pipeline]++;
  });
  Object.entries(byPipeline).sort((a,b) => b[1] - a[1]).forEach(([p, n]) => console.log(`  ${p}: ${n}`));
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
