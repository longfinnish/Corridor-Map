/**
 * Place operator-confirmed gas interconnect points on actual pipeline geometry.
 * Two-pass approach:
 *   Pass 1: Collect all HIFLD refs + EIA line segments per pipeline+county
 *   Pass 2: Distribute points along available geometry within each county
 */
const fs = require('fs');
const path = require('path');

const DATA = path.join(__dirname, 'data');

// Load data
const gi = JSON.parse(fs.readFileSync(path.join(DATA, 'gas_interconnects.json'), 'utf-8'));
const hifldPoints = JSON.parse(fs.readFileSync(path.join(DATA, 'hifld_pmtiles_points.json'), 'utf-8'));
const eiaSegments = JSON.parse(fs.readFileSync(path.join(DATA, 'eia_pipeline_segments.json'), 'utf-8'));

console.log(`Loaded: ${gi.pipelines.length} pipelines, ${hifldPoints.length} HIFLD points, ${Object.keys(eiaSegments).length} EIA operators`);

// ============================================================
// COMPANY NAME MAPPING
// ============================================================
const pipelineToHIFLD = {
  'Florida Gas': ['FLORIDA GAS TRANSMISSION'],
  'Texas Eastern': ['TEXAS EASTERN TRANSMISSION'],
  'Algonquin': ['ALGONQUIN GAS TRANSMISSION'],
  'East Tennessee': ['EAST TENNESSEE NATURAL GAS'],
  'Gulfstream ENB': ['GULFSTREAM NATURAL GAS SYSTEM'],
  'Gulfstream': ['GULFSTREAM NATURAL GAS SYSTEM'],
  'Panhandle Eastern': ['PANHANDLE EASTERN PIPE LINE'],
  'Trunkline': ['TRUNKLINE GAS COMPANY'],
  'Rover': ['ROVER PIPELINE LLC'],
  'Transwestern': ['TRANSWESTERN PIPELINE'],
  'Tiger': ['TIGER PIPELINE'],
  'Sea Robin': ['SEA ROBIN PIPELINE'],
  'Enable MRT': ['ENABLE GAS TRANSMISSION', 'ENABLE MISSISSIPPI RIVER TRANSMISSION'],
  'CenterPoint EGT': ['CENTERPOINT ENERGY GAS TRANSMISSION COMPANY'],
  'Northern Natural': ['NORTHERN NATURAL GAS COMPANY'],
  'Tennessee Gas': ['TENNESSEE GAS PIPELINE'],
  'NGPL': ['NATURAL GAS PIPELINE (KINDER MORGAN)', 'NATURAL GAS PL CO OF AM'],
  'El Paso': ['EL PASO NATURAL GAS', 'EL PASO TEXAS PIPELINE CO'],
  'Southern Natural': ['SOUTHERN NATURAL GAS COMPANY LLC'],
  'Colorado Interstate': ['COLORADO INTERSTATE GAS COMPANY LLC (KINDER MORGAN)'],
  'Midcontinent Express': ['MIDCONTINENT EXPRESS PIPELINE'],
  'KM Louisiana': ['KM INTERSTATE GAS COMPANY'],
  'KM Illinois': ['KM INTERSTATE GAS COMPANY'],
  'WIC': ['WYOMING INTERSTATE'],
  'TransColorado': ['TRANSCOLORADO GAS TRANSMISSION'],
  'Cheyenne Plains': ['CHEYENNE PLAINS INVESTMENT COMPANY'],
  'Mojave': ['MOJAVE PIPELINE COMPANY'],
  'Sierrita': ['SIERRITA GAS PIPELINE'],
  'Elba Express': ['ELBA EXPRESS COMPANY'],
  'Stagecoach': ['STAGECOACH GAS SERVICES'],
  'Columbia Gas': ['COLUMBIA GAS TRANS CO', 'COLUMBIA GAS TRANSMISSION'],
  'Columbia Gulf': ['COLUMBIA GULF TRANSMISSION'],
  'ANR': ['ANR PIPELINE COMPANY'],
  'ANR Storage': ['ANR PIPELINE COMPANY'],
  'Bison': ['BISON PIPELINE LLC'],
  'Blue Lake': ['BLUE LAKE GAS STORAGE'],
  'Crossroads': ['CROSSROADS PIPELINE'],
  'Millennium': ['MILLENNIUM PIPELINE'],
  'Northern Border': ['NORTHERN BORDER PIPELINE'],
  'Equitrans': ['EQUITRANS LP'],
  'National Fuel Gas': ['NATIONAL FUEL GAS SUPPLY'],
  'Northwest': ['NORTHWEST PIPELINE LLC'],
  'MountainWest': ['QUESTAR PIPELINE', 'MOUNTAINWEST PIPELINES'],
  'MountainWest Overthrust': ['QUESTAR PIPELINE', 'OVERTHRUST PIPELINE'],
  'Transco': ['TRANSCONTINENTAL GAS PL', 'TRANSCO'],
  'Texas Gas': ['TEXAS GAS TRANSMISSION'],
  'AlaTenn': ['AMERICAN MIDSTREAM (ALATENN)'],
  'Midla': ['AMERICAN MIDSTREAM (MIDLA)'],
  'Ozark Gas': ['OZARK GAS TRANSMISSION'],
  'Great Lakes': ['GREAT LAKES GAS TRANSMISSION'],
  'GTN': ['GAS TRANSMISSION NORTHWEST'],
  'WBI Energy': ['WBI ENERGY TRANSMISSION'],
  'North Baja': ['NORTH BAJA PIPELINE'],
  'Mountain Valley': ['MOUNTAIN VALLEY PIPELINE'],
};

const pipelineToEIA = {
  'Florida Gas': ['Florida Gas Trans Co'],
  'Texas Eastern': ['Texas Eastern Trans Co'],
  'Algonquin': ['Algonquin Gas Trans Co'],
  'East Tennessee': ['East Tennessee Nat Gas Co'],
  'Panhandle Eastern': ['Panhandle Eastern PL Co'],
  'Trunkline': ['Trunkline Gas Co'],
  'CenterPoint EGT': ['Enable Gas Transmission', 'CenterPoint Energy'],
  'Northern Natural': ['Northern Natural Gas Co'],
  'Tennessee Gas': ['Tennessee Gas Pipeline'],
  'NGPL': ['Natural Gas PL Co of Am'],
  'El Paso': ['El Paso Natural Gas Co', 'El Paso Texas Pipeline Co'],
  'Southern Natural': ['Southern Natural Gas Co'],
  'Colorado Interstate': ['Colorado Interstate Gas Co'],
  'WIC': ['Wyoming Interstate Co'],
  'TransColorado': ['TransColorado Gas Transmission'],
  'Cheyenne Plains': ['Cheyenne Plains Investment Co'],
  'Mojave': ['Mojave Pipeline Co'],
  'Columbia Gas': ['Columbia Gas Trans Co'],
  'Columbia Gulf': ['Columbia Gulf Transmission Co'],
  'ANR': ['ANR Pipeline Co'],
  'Northern Border': ['Northern Border Pipeline Co'],
  'Northwest': ['Northwest Pipeline Co', 'Northwest Pipeline LLC'],
  'Transco': ['Transcontinental Gas PL'],
  'Texas Gas': ['Texas Gas Transmission'],
  'Great Lakes': ['Great Lakes Gas Transmission'],
  'National Fuel Gas': ['National Fuel Gas Supply Corp'],
  'Millennium': ['Millennium Pipeline Co'],
  'Equitrans': ['Equitrans LP'],
  'Midcontinent Express': ['Midcontinent Express Pipeline'],
  'KM Louisiana': ['Kinder Morgan Louisiana Pipeline'],
  'Stagecoach': ['Stagecoach Gas Services'],
  'Elba Express': ['Elba Express Co'],
};

// ============================================================
// HELPERS
// ============================================================
function norm(s) { return (s || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Collect all coordinates from EIA line segments near a given point (within bbox)
function getEIALinePointsNear(lat, lon, eiaLines, radiusDeg = 0.5) {
  const points = [];
  for (const line of eiaLines) {
    for (const [lnCoord, ltCoord] of line) {
      if (Math.abs(ltCoord - lat) < radiusDeg && Math.abs(lnCoord - lon) < radiusDeg) {
        points.push({ lat: ltCoord, lon: lnCoord });
      }
    }
  }
  return points;
}

// Interpolate a point along a polyline at fraction t (0-1)
function interpolateAlongLine(linePoints, t) {
  if (linePoints.length === 0) return null;
  if (linePoints.length === 1) return linePoints[0];

  // Calculate total length
  let totalLen = 0;
  const segLens = [];
  for (let i = 0; i < linePoints.length - 1; i++) {
    const d = haversine(linePoints[i].lat, linePoints[i].lon, linePoints[i + 1].lat, linePoints[i + 1].lon);
    segLens.push(d);
    totalLen += d;
  }
  if (totalLen === 0) return linePoints[0];

  const targetDist = t * totalLen;
  let accum = 0;
  for (let i = 0; i < segLens.length; i++) {
    if (accum + segLens[i] >= targetDist) {
      const frac = (targetDist - accum) / segLens[i];
      return {
        lat: linePoints[i].lat + frac * (linePoints[i + 1].lat - linePoints[i].lat),
        lon: linePoints[i].lon + frac * (linePoints[i + 1].lon - linePoints[i].lon),
      };
    }
    accum += segLens[i];
  }
  return linePoints[linePoints.length - 1];
}

// ============================================================
// BUILD LOOKUPS
// ============================================================
console.log('\nBuilding lookups...');

// HIFLD: company+county+state -> [points]
const hifldLookup = {};
hifldPoints.forEach(p => {
  const company = norm(p.company || '');
  const county = norm(p.county || '');
  const state = (p.state || '').toUpperCase().trim();
  const key = `${company}|${county}|${state}`;
  if (!hifldLookup[key]) hifldLookup[key] = [];
  hifldLookup[key].push(p);
});

// HIFLD by company+state for broader search
const hifldByCompanyState = {};
hifldPoints.forEach(p => {
  const company = norm(p.company || '');
  const state = (p.state || '').toUpperCase().trim();
  const key = `${company}|${state}`;
  if (!hifldByCompanyState[key]) hifldByCompanyState[key] = [];
  hifldByCompanyState[key].push(p);
});

// EIA: normalized operator -> array of coordinate lines
const eiaByOperator = {};
for (const [operator, segs] of Object.entries(eiaSegments)) {
  const normOp = norm(operator);
  if (!eiaByOperator[normOp]) eiaByOperator[normOp] = [];
  segs.forEach(seg => {
    seg.coords.forEach(line => {
      if (line.length >= 2) eiaByOperator[normOp].push(line);
    });
  });
}

console.log(`  ${Object.keys(hifldLookup).length} HIFLD company+county+state combos`);
console.log(`  ${Object.keys(eiaByOperator).length} EIA operators`);

// ============================================================
// PLACEMENT
// ============================================================
console.log('\n=== Placing points on geometry ===\n');

const stats = {
  total: 0,
  targets: 0,
  hifld_matched: 0,
  eia_placed: 0,
  still_confirmed: 0,
  moves: [],
};

gi.pipelines.forEach(pipeline => {
  const pName = pipeline.short || pipeline.name;
  const hifldCompanies = pipelineToHIFLD[pName] || [];
  const eiaOperators = pipelineToEIA[pName] || [];

  // Get all EIA lines for this pipeline
  const pipelineEIALines = [];
  for (const eiaOp of eiaOperators) {
    const lines = eiaByOperator[norm(eiaOp)] || [];
    pipelineEIALines.push(...lines);
  }

  // Group target points by county
  const byCounty = {};
  pipeline.points.forEach((pt, idx) => {
    stats.total++;
    if (pt.geocoding_quality !== 'operator_confirmed_county' && pt.geocoding_quality !== 'county_dispersed') return;
    stats.targets++;
    const county = norm(pt.county || '');
    const state = (pt.state || '').toUpperCase().trim();
    const key = `${county}|${state}`;
    if (!byCounty[key]) byCounty[key] = { county, state, points: [] };
    byCounty[key].points.push({ pt, idx });
  });

  let pHifld = 0, pEia = 0;

  for (const [key, group] of Object.entries(byCounty)) {
    const { county, state, points } = group;

    // Collect available HIFLD reference points in this county
    const hifldRefs = [];
    for (const company of hifldCompanies) {
      const hKey = `${norm(company)}|${county}|${state}`;
      if (hifldLookup[hKey]) hifldRefs.push(...hifldLookup[hKey]);
    }

    // Collect EIA line vertices near this county (use first point's coords as center)
    const centerLat = points[0].pt.lat;
    const centerLon = points[0].pt.lng;
    const eiaVertices = getEIALinePointsNear(centerLat, centerLon, pipelineEIALines, 0.5);

    // Combine all available placement points
    const placementPoints = [];
    hifldRefs.forEach(r => placementPoints.push({ lat: r.lat, lon: r.lon, src: 'hifld' }));
    eiaVertices.forEach(v => placementPoints.push({ lat: v.lat, lon: v.lon, src: 'eia' }));

    if (placementPoints.length === 0) {
      // No geometry available — try wider state search for EIA
      const widerEIA = getEIALinePointsNear(centerLat, centerLon, pipelineEIALines, 1.5);
      widerEIA.forEach(v => placementPoints.push({ lat: v.lat, lon: v.lon, src: 'eia' }));
    }

    if (placementPoints.length === 0) {
      // Still nothing — keep as-is
      stats.still_confirmed += points.length;
      continue;
    }

    // Sort placement points to form a rough line (by lon or lat depending on orientation)
    const latRange = Math.max(...placementPoints.map(p => p.lat)) - Math.min(...placementPoints.map(p => p.lat));
    const lonRange = Math.max(...placementPoints.map(p => p.lon)) - Math.min(...placementPoints.map(p => p.lon));
    if (latRange > lonRange) {
      placementPoints.sort((a, b) => a.lat - b.lat);
    } else {
      placementPoints.sort((a, b) => a.lon - b.lon);
    }

    // Determine quality: mostly HIFLD or mostly EIA?
    const hifldCount = placementPoints.filter(p => p.src === 'hifld').length;
    const quality = hifldCount > 0 ? 'hifld_matched' : 'eia_geometry_placed';

    // Distribute points evenly along the placement geometry
    const numPoints = points.length;
    for (let i = 0; i < numPoints; i++) {
      const { pt } = points[i];
      const oldLat = pt.lat;
      const oldLng = pt.lng;

      // Use seeded index based on point ID for reproducibility
      const seed = parseInt(pt.id || '0', 10) || pt.name.length;

      let newCoord;
      if (numPoints === 1) {
        // Single point: place at the center reference
        const center = placementPoints[Math.floor(placementPoints.length / 2)];
        newCoord = { lat: center.lat, lon: center.lon };
      } else if (placementPoints.length >= numPoints) {
        // More placement points than target points: pick evenly spaced ones
        const idx = Math.floor(i * (placementPoints.length - 1) / (numPoints - 1));
        newCoord = placementPoints[idx];
      } else {
        // Fewer placement points: interpolate along the line
        const t = numPoints > 1 ? i / (numPoints - 1) : 0.5;
        newCoord = interpolateAlongLine(placementPoints, t);
      }

      if (!newCoord) {
        stats.still_confirmed++;
        continue;
      }

      // Add small seeded jitter to prevent exact overlap (±0.001° ≈ ±100m)
      const jLat = ((seed * 7919 + i * 3571) % 2000 - 1000) / 1000000;
      const jLon = ((seed * 6271 + i * 2909) % 2000 - 1000) / 1000000;

      const newLat = Math.round((newCoord.lat + jLat) * 100000) / 100000;
      const newLng = Math.round((newCoord.lon + jLon) * 100000) / 100000;
      const moveDist = haversine(oldLat, oldLng, newLat, newLng);

      pt.lat = newLat;
      pt.lng = newLng;
      pt.geocoding_quality = quality;
      pt.loc_accuracy = quality === 'hifld_matched' ? 'hifld_point' : 'pipeline_snap';
      stats.moves.push(moveDist);

      if (quality === 'hifld_matched') { pHifld++; stats.hifld_matched++; }
      else { pEia++; stats.eia_placed++; }
    }
  }

  if (pHifld + pEia > 0) {
    console.log(`  ${pName}: ${pHifld} HIFLD, ${pEia} EIA`);
  }
});

// ============================================================
// SUMMARY
// ============================================================
const avgMove = stats.moves.length > 0
  ? (stats.moves.reduce((a, b) => a + b, 0) / stats.moves.length).toFixed(1) : 0;
const sortedMoves = stats.moves.sort((a, b) => a - b);
const medianMove = sortedMoves.length > 0
  ? sortedMoves[Math.floor(sortedMoves.length / 2)].toFixed(1) : 0;

console.log(`\n=== Placement Summary ===`);
console.log(`  Total: ${stats.total}, Targets: ${stats.targets}`);
console.log(`  HIFLD matched: ${stats.hifld_matched}`);
console.log(`  EIA placed: ${stats.eia_placed}`);
console.log(`  Still confirmed (no geometry): ${stats.still_confirmed}`);
console.log(`  Avg move: ${avgMove} km, Median: ${medianMove} km`);

// Final quality breakdown
const finalQuality = {};
let totalPts = 0;
gi.pipelines.forEach(p => {
  p.points.forEach(pt => {
    totalPts++;
    const q = pt.geocoding_quality || 'none';
    finalQuality[q] = (finalQuality[q] || 0) + 1;
  });
});

console.log('\n=== Final Quality Breakdown ===');
Object.entries(finalQuality).sort((a, b) => b[1] - a[1]).forEach(([q, n]) => {
  console.log(`  ${q}: ${n} (${(n / totalPts * 100).toFixed(1)}%)`);
});

// Check remaining clustering
let clusters10plus = 0;
const coordCounts = {};
gi.pipelines.forEach(p => p.points.forEach(pt => {
  if (!pt.lat || !pt.lng) return;
  const k = `${pt.lat.toFixed(2)},${pt.lng.toFixed(2)}`;
  coordCounts[k] = (coordCounts[k] || 0) + 1;
}));
Object.values(coordCounts).forEach(n => { if (n >= 10) clusters10plus++; });
console.log(`\nClusters ≥10 at 0.01° resolution: ${clusters10plus}`);

// Save
fs.writeFileSync(path.join(DATA, 'gas_interconnects.json'), JSON.stringify(gi, null, 2) + '\n');
console.log('\nSaved gas_interconnects.json');

const report = {
  timestamp: new Date().toISOString(),
  total_points: totalPts,
  targets: stats.targets,
  hifld_matched: stats.hifld_matched,
  eia_geometry_placed: stats.eia_placed,
  still_operator_confirmed: stats.still_confirmed,
  avg_move_km: parseFloat(avgMove),
  median_move_km: parseFloat(medianMove),
  hifld_reference_points_available: hifldPoints.length,
  eia_operators_available: Object.keys(eiaSegments).length,
  remaining_clusters_10plus: clusters10plus,
  final_quality_breakdown: finalQuality,
};
fs.writeFileSync(path.join(DATA, 'geocoding_placement_report.json'), JSON.stringify(report, null, 2) + '\n');
console.log('Saved geocoding_placement_report.json');
