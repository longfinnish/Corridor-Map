export async function onRequest(context) {
  const url = new URL(context.request.url);
  const lat = url.searchParams.get('lat');
  const lon = url.searchParams.get('lon');
  
  if (!lat || !lon) {
    return new Response(JSON.stringify({error:'lat and lon required'}), {
      status:400, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
    });
  }

  try {
    // Step 1: search for parcel at coordinates
    const searchUrl = 'https://app.regrid.com/search.json?query=' + lat + ',' + lon + '&context=/us';
    const searchRes = await fetch(searchUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': 'https://app.regrid.com/',
        'Accept': 'application/json'
      }
    });
    
    if (!searchRes.ok) {
      return new Response(JSON.stringify({error:'search failed', status:searchRes.status}), {
        status:502, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
      });
    }
    
    const results = await searchRes.json();
    if (!results || !results.length || !results[0].path) {
      return new Response(JSON.stringify({error:'no parcel found'}), {
        status:404, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
      });
    }

    // Step 2: fetch parcel detail
    const detailUrl = 'https://app.regrid.com' + results[0].path + '.json';
    const detailRes = await fetch(detailUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': 'https://app.regrid.com/',
        'Accept': 'application/json'
      }
    });
    
    if (!detailRes.ok) {
      return new Response(JSON.stringify({error:'detail failed', status:detailRes.status}), {
        status:502, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
      });
    }

    const detail = await detailRes.json();
    return new Response(JSON.stringify(detail), {
      headers: {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
    });

  } catch (e) {
    return new Response(JSON.stringify({error:e.message}), {
      status:500, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
    });
  }
}
