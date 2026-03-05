# Corridor Map — Data Sources & Refresh Guide
*Last updated: March 4, 2026 — v2.5.0*

## Fiber Routes

### Infrapedia (live MVT tiles)
- **Endpoint:** `https://www.infrapedia.com/map/cables/{z}/{x}/{y}.pbf`
- **CORS:** Yes (`access-control-allow-origin: *`)
- **Auth:** None required
- **Layer name in tiles:** `cables`
- **Key fields:** name, status, category (filter `category=terrestrial`)
- **Carriers:** 129 networks — Zayo, Windstream, Crown Castle, Fiberlight, etc.
- **Missing:** Lumen, AT&T, Verizon (not crowd-sourced into Infrapedia)
- **Backup:** `data/fiber_routes.json` (1.3MB, 41 carriers, extracted zoom 5)
- **Refresh:** Re-download tiles at zoom 5 (x=5-9, y=9-13) using `mapbox-vector-tile` Python library

### Lumen / Level3 / CenturyLink (static GeoJSON)
- **Source:** `atlas.lumen.com` Mapbox tile server
- **Tile URL:** `https://atlas.lumen.com/v4/atlas-user.ctl-route/{z}/{x}/{y}.vector.pbf?access_token={TOKEN}`
- **Token:** `[LUMEN_MAPBOX_TOKEN - see project knowledge]`
- **Auth:** Requires `Referer: https://www.lumen.com/` + browser User-Agent headers. Token is URL-restricted.
- **Tileset ID:** `atlas-user.ctl-route`
- **Other tilesets available:** `atlas-user.DataCentersCombined-20220921`, `atlas-user.EdgeSiteLocations-20220926-F`, `atlas-user.ctl-buildings-v2`
- **Layer name:** `CENTURYLINK_ROUTE`
- **Key fields:** FIBER_STAT, INSTALLATI, STATE, LEGACY_OWN, ROUTE_NAME, NETWORK_BU, IS_LEVEL3, IS_WILTEL
- **File:** `data/lumen_routes.json` (2.4MB, 684 feature groups, 53K+ routes)
- **Extracted:** March 4, 2026 at zoom 5, simplified 0.008°
- **Refresh:** Python script with `mapbox-vector-tile`, `requests`, `gzip`. Requires Referer+UA headers.

### AT&T (NOT YET EXTRACTED)
- **Approach:** Use F12 Network tab on AT&T's network map page to find tile server URL
- **AT&T map page:** `https://www.business.att.com/products/fiber-network-map.html` or similar
- **Same technique as Lumen:** look for `.vector.pbf` or `.pbf` requests, get full URL, extract tileset ID and token

## Gas Infrastructure
- **Base URL:** `https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/`
- Compressor Stations, Receipt/Delivery Points, Processing Plants, Storage Facilities, Interconnects
- **CORS:** Yes (ArcGIS Online)
- **Refresh:** Live query — no action needed

## Parcels
| State | Endpoint | Owner | Value | CORS |
|-------|----------|-------|-------|------|
| TX | `services1.arcgis.com/.../2019_Texas_Parcels_StratMap/FeatureServer/0` | Yes | No | Yes |
| WI | `services3.arcgis.com/.../Wisconsin_Statewide_Parcels/FeatureServer/0` | Yes | Yes | Yes |
| MN | `services.arcgis.com/9OIuDHbyhmH91RfZ/.../plan_parcels_open_gdb/FeatureServer/0` | Yes | No | Yes |
| IN | `gisdata.in.gov/.../Parcel_Boundaries_of_Indiana_Current/FeatureServer/0` | No | No | Yes |
| All US | Regrid tiles: `tiles.arcgis.com/.../Regrid_Nationwide_Parcel_Boundaries_v1/MapServer/tile/{z}/{y}/{x}` | No (boundaries only) | No | Yes |

### States NOT found (statewide with owner):
LA, MS, OK, AR, IL, MO, AL, MI, KY, TN, KS, NE — now covered via Regrid JSON lookup (see below)

### Regrid Parcel Lookup (nationwide, unauthenticated)
- **Search endpoint:** `https://app.regrid.com/search.json?query={lat},{lon}&context=/us`
- **Detail endpoint:** `https://app.regrid.com/us/{state}/{county}/{city}/{id}.json`
- **Auth:** None — fully open, no API key needed
- **Rate limit:** Self-imposed 1.2s between calls to stay under radar
- **Returns:** Owner name, mailing address, assessed value, land value, sale price/date, acreage, zoning, use description, year built, parcel geometry
- **Coverage:** All 50 states — replaces need for state-by-state ArcGIS parcel layers for owner data
- **Integration:** Click handler on Regrid tile layer at zoom 14+ → search.json → detail.json → popup

### Regrid API Token (sandbox — does NOT return data):
`[REGRID_SANDBOX_TOKEN - see project knowledge]`

### Regrid Paid API ($375/mo — not yet active)
- **What it solves:** National queryable FeatureServer — search parcels by acreage, owner, zoning, value, use code across all 50 states. Enables "show me every 200+ acre parcel in this county" queries and draw-a-zone spatial search.
- **What it doesn't solve:** Proximity to infrastructure (transmission, gas, fiber). That requires chaining Regrid results with HIFLD/EIA/Infrapedia spatial queries — custom code regardless of tier.
- **When to upgrade:** When you're actively screening parcels and manually scanning the map feels slow. The trigger is needing to filter parcels by criteria across a county, not just clicking individual ones.
- **Growth path:** Backbone of an automated site screening pipeline — feed in criteria, get ranked parcels. Scales to multiple developer clients.
- **Free tier ceiling:** Click-to-copy-coords workflow handles 5-10 site evaluations. Breaks down for bulk screening.

## Other Live Layers
- **Transmission Lines:** HIFLD `services1.arcgis.com/.../Electric_Power_Transmission_Lines/FeatureServer/0`
- **Gas Pipelines (EIA):** `geo.dot.gov/.../Natural_Gas_Pipelines_US_EIA/FeatureServer/0`
- **Gas Pipelines (OSM):** Overpass API `way["man_made"="pipeline"]["substance"="gas"]`
- **Data Centers (OSM):** Overpass API `node/way["telecom"="data_center"]`
- **FEMA Flood:** `hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer` layer 28
- **NWI Wetlands:** `fwspublicservices.wim.usgs.gov/.../Wetlands/MapServer` layer 0
- **County Boundaries:** `services.arcgis.com/P3ePLMYs2RVChkJx/.../USA_Counties_Generalized_Boundaries/FeatureServer/0`
- **State Boundaries:** `services.arcgis.com/P3ePLMYs2RVChkJx/.../USA_States_Generalized_Boundaries/FeatureServer/0`

## Extraction Tools
- Python: `mapbox-vector-tile`, `requests`, `gzip`
- Key pattern: download PBF tiles → gzip decompress → decode MVT → convert pixel coords to lat/lon → simplify → export GeoJSON
