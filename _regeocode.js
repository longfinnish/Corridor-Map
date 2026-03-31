const fs = require('fs');
const path = require('path');

const DATA = path.join(__dirname, 'data');
const LOCS_DIR = path.join(DATA, 'operator_locations');

// Load operator locations
const opLocs = JSON.parse(fs.readFileSync(path.join(LOCS_DIR, '_all_operator_locations.json'), 'utf-8'));
console.log(`Loaded ${opLocs.length} operator locations`);

// Load gas interconnects
const gi = JSON.parse(fs.readFileSync(path.join(DATA, 'gas_interconnects.json'), 'utf-8'));
const pipelines = gi.pipelines;

// Load HIFLD reference points (from geocoding audit)
let hifldRefs = [];
const hifldPath = path.join(DATA, 'hifld_reference_points.json');
if (fs.existsSync(hifldPath)) {
  hifldRefs = JSON.parse(fs.readFileSync(hifldPath, 'utf-8'));
  console.log(`Loaded ${hifldRefs.length} HIFLD reference points`);
}

// Build lookup: pipeline_code -> array of operator locations
const opByPipeline = {};
opLocs.forEach(loc => {
  const code = loc.pipeline_code;
  if (!opByPipeline[code]) opByPipeline[code] = [];
  opByPipeline[code].push(loc);
});

// Map gas_interconnects pipeline names to operator pipeline codes
const pipelineCodeMap = {
  'Florida Gas': 'FGT',
  'Panhandle Eastern': 'PEPL',
  'Trunkline': 'TGC',
  'Transwestern': 'TW',
  'Tiger': 'TGR',
  'Sea Robin': 'SPC',
  'Rover': 'ROVER',
  'Enable MRT': 'MRT',
  'CenterPoint EGT': 'EGT',
  'Northern Natural': 'NNG',
  'Tennessee Gas': 'TGP',
  'NGPL': 'NGPL',
  'El Paso': 'EPNG',
  'Southern Natural': 'SNG',
  'Colorado Interstate': 'CIG',
  'Midcontinent Express': 'MEP',
  'KM Louisiana': 'KMLP',
  'KM Illinois': 'KMIL',
  'WIC': 'WIC',
  'TransColorado': 'TCP',
  'Cheyenne Plains': 'CP',
  'Mojave': 'MOPC',
  'Sierrita': 'SGP',
  'Elba Express': 'EEC',
  'Stagecoach': 'STAG',
  'Arlington Storage': 'ARLS',
  'Young Gas Storage': 'YGS',
  'KM Texas': 'KMTP',
  'KM Tejas': 'KMTJ',
  'KM North Texas': 'KMNT',
  'GCX': 'GCX',
  'PHP': 'PHP',
  'KM Border': 'KMBP',
  'NET Mexico': 'NETM',
  'Eagle Ford': 'KMEF',
  'Twin Tier': 'TTP',
  // TC Energy
  'Columbia Gas': 'CGTC',
  'Columbia Gulf': 'CGUL',
  'ANR': 'ANR',
  'ANR Storage': 'ANRS',
  'Bison': 'BISN',
  'Blue Lake': 'BGLK',
  'Crossroads': 'CRPK',
  'ERGSS': 'ERGS',
  'Hardy Storage': 'HRDY',
  'Millennium': 'MILL',
  'Northern Border': 'NBRD',
  'TC Louisiana': 'TCLP',
  // BBT/Quorum
  'AlaTenn': 'ALTN',
  'Midla': 'MIDL',
  'Trans-Union': 'TRUN',
  'Ozark Gas': 'OZRK',
};

// Haversine distance in km
function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Normalize string for fuzzy matching
function norm(s) {
  return (s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

// US state FIPS to county centroids (approximate) - we'll use HIFLD refs instead
// Build HIFLD reference by pipeline+state+county for Tier 3
const hifldByKey = {};
hifldRefs.forEach(ref => {
  const key = `${norm(ref.pipeline)}|${norm(ref.state)}`;
  if (!hifldByKey[key]) hifldByKey[key] = [];
  hifldByKey[key].push(ref);
});

// Stats
const stats = {
  total_points: 0,
  already_good: 0,
  matched_by_id: 0,
  matched_by_name: 0,
  county_improved: 0,
  hifld_county_snap: 0,
  suspect_unfixed: 0,
  operator_location_files: [],
};

// For each pipeline in gas_interconnects, try to match and improve coordinates
console.log('\n=== Matching operator locations to gas interconnects ===\n');

pipelines.forEach(pipeline => {
  const pName = pipeline.short || pipeline.name;
  const pCode = pipelineCodeMap[pName];
  const opData = pCode ? (opByPipeline[pCode] || []) : [];

  // Also check connected pipelines - a point named "WIC/CIG" on CIG's list might match WIC's point
  // Build op lookup by point_id and normalized point_name for this pipeline
  const opById = {};
  const opByName = {};
  opData.forEach(loc => {
    if (loc.point_id) opById[loc.point_id] = loc;
    const n = norm(loc.point_name);
    if (n && !opByName[n]) opByName[n] = loc;
  });

  // Also build a lookup from ALL operator data for cross-pipeline matching
  // (a CIG point might reference WIC locations)

  let matched = 0;
  let improved = 0;

  pipeline.points.forEach(pt => {
    stats.total_points++;

    // Skip points that are already good
    if (pt.geocoding_quality === 'hifld_exact') {
      stats.already_good++;
      return;
    }
    if (pt.geocoding_quality === 'original' && pt.loc_accuracy !== 'county') {
      stats.already_good++;
      return;
    }

    // Try match by point ID
    let opMatch = null;
    if (pt.id && opById[pt.id]) {
      opMatch = opById[pt.id];
      stats.matched_by_id++;
    }

    // Try match by name
    if (!opMatch && pt.name) {
      const ptNorm = norm(pt.name);
      if (opByName[ptNorm]) {
        opMatch = opByName[ptNorm];
        stats.matched_by_name++;
      }

      // Try partial name match - search all op locations for this pipeline
      if (!opMatch) {
        for (const loc of opData) {
          const locNorm = norm(loc.point_name);
          if (locNorm && ptNorm && (locNorm.includes(ptNorm) || ptNorm.includes(locNorm))) {
            opMatch = loc;
            stats.matched_by_name++;
            break;
          }
        }
      }

      // Try matching across all operator data (cross-pipeline)
      if (!opMatch) {
        for (const loc of opLocs) {
          if (loc.point_id === pt.id) {
            opMatch = loc;
            stats.matched_by_id++;
            break;
          }
        }
      }
    }

    if (opMatch) {
      matched++;
      // We have authoritative county+state from operator
      const opCounty = (opMatch.county || '').trim().toUpperCase();
      const opState = (opMatch.state || '').trim().toUpperCase();
      const ptCounty = (pt.county || '').trim().toUpperCase();
      const ptState = (pt.state || '').trim().toUpperCase();

      // If county/state differ, the point was likely at wrong county centroid
      const countyChanged = opCounty && opCounty !== ptCounty;

      if (opCounty && opState) {
        // Update county/state from authoritative source
        pt.county = opMatch.county.trim();
        pt.state = opMatch.state.trim();

        // Try to find HIFLD reference points in the correct county for better placement
        const hKey = `${norm(pName)}|${opState}`;
        const hRefs = hifldByKey[hKey] || [];

        // Find refs in the matching county
        const countyRefs = hRefs.filter(r =>
          norm(r.county || '').includes(opCounty) || opCounty.includes(norm(r.county || ''))
        );

        if (countyRefs.length > 0) {
          // Place near the HIFLD reference point in the correct county
          // If multiple, use the centroid of all refs in that county
          const avgLat = countyRefs.reduce((s, r) => s + r.lat, 0) / countyRefs.length;
          const avgLon = countyRefs.reduce((s, r) => s + r.lon, 0) / countyRefs.length;

          // Add small jitter based on point ID to prevent stacking
          const seed = parseInt(pt.id || '0', 10) || pt.name.length;
          const jitterLat = ((seed * 7919) % 1000 - 500) / 100000; // ~0.005 degree max
          const jitterLon = ((seed * 6271) % 1000 - 500) / 100000;

          pt.lat = Math.round((avgLat + jitterLat) * 100000) / 100000;
          pt.lng = Math.round((avgLon + jitterLon) * 100000) / 100000;
          pt.geocoding_quality = 'operator_county_hifld_snap';
          pt.loc_accuracy = 'county_hifld';
          improved++;
          stats.hifld_county_snap++;
        } else if (countyChanged) {
          // County changed but no HIFLD ref in new county — need to estimate position
          // Use state-level HIFLD refs and find closest to county area
          const stateRefs = hRefs;
          if (stateRefs.length > 0) {
            // Pick a ref point and jitter
            const refIdx = (parseInt(pt.id || '0', 10) || pt.name.length) % stateRefs.length;
            const ref = stateRefs[refIdx];
            const seed = parseInt(pt.id || '0', 10) || pt.name.length;
            const jitterLat = ((seed * 7919) % 2000 - 1000) / 100000;
            const jitterLon = ((seed * 6271) % 2000 - 1000) / 100000;

            pt.lat = Math.round((ref.lat + jitterLat) * 100000) / 100000;
            pt.lng = Math.round((ref.lon + jitterLon) * 100000) / 100000;
            pt.geocoding_quality = 'operator_county_state_snap';
            pt.loc_accuracy = 'state_approx';
            improved++;
            stats.county_improved++;
          } else {
            pt.geocoding_quality = 'operator_county_no_ref';
            stats.suspect_unfixed++;
          }
        } else {
          // Same county, operator confirmed it — mark quality upgrade
          if (pt.geocoding_quality === 'county_dispersed') {
            pt.geocoding_quality = 'operator_confirmed_county';
          }
          stats.county_improved++;
        }
      }
    } else {
      // No operator match — leave as-is
      if (pt.geocoding_quality === 'suspect_unfixed') {
        stats.suspect_unfixed++;
      }
    }
  });

  if (opData.length > 0) {
    console.log(`  ${pName} (${pCode}): ${opData.length} operator locs, ${matched} matched, ${improved} improved`);
    stats.operator_location_files.push({
      pipeline: pName,
      pipeline_code: pCode,
      source: opData[0]?.source || 'unknown',
      points_in_file: opData.length,
      matched_to_map: matched,
      coordinates_improved: improved,
    });
  }
});

// Save updated gas_interconnects.json
fs.writeFileSync(path.join(DATA, 'gas_interconnects.json'), JSON.stringify(gi, null, 2) + '\n');
console.log('\nSaved updated gas_interconnects.json');

// Generate quality report
const finalQuality = {};
let totalPts = 0;
pipelines.forEach(p => {
  p.points.forEach(pt => {
    totalPts++;
    const q = pt.geocoding_quality || 'none';
    finalQuality[q] = (finalQuality[q] || 0) + 1;
  });
});

const report = {
  total_points: totalPts,
  geocoding_quality_breakdown: finalQuality,
  operator_data_summary: {
    total_operator_locations_pulled: opLocs.length,
    sources: {
      kinder_morgan: opLocs.filter(l => l.source === 'kinder_morgan').length,
      northern_natural: opLocs.filter(l => l.source === 'northern_natural').length,
      energy_transfer: opLocs.filter(l => l.source === 'energy_transfer').length,
    },
  },
  matching_stats: stats,
  operator_location_files: stats.operator_location_files,
};

fs.writeFileSync(path.join(DATA, 'geocoding_fix_report.json'), JSON.stringify(report, null, 2) + '\n');
console.log('Saved geocoding_fix_report.json');

console.log('\n=== Final Quality Breakdown ===');
Object.entries(finalQuality).sort((a, b) => b[1] - a[1]).forEach(([q, n]) => {
  console.log(`  ${q}: ${n} (${(n / totalPts * 100).toFixed(1)}%)`);
});
