// Pipeline API — CRUD for folders and sites stored in Cloudflare KV
// KV key: "pipeline" — stores the entire pipeline as one JSON blob
// Structure: { folders: [ { id, name, created, sites: [ { id, name, lat, lng, county, state, stage, notes, created, updated } ] } ] }

export async function onRequest(context) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (context.request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const KV = context.env.PIPELINE;
  if (!KV) {
    return new Response(JSON.stringify({error: 'KV not configured'}), {
      status: 500, headers: {'Content-Type': 'application/json', ...corsHeaders}
    });
  }

  try {
    // GET — return full pipeline
    if (context.request.method === 'GET') {
      const data = await KV.get('pipeline', 'json');
      return new Response(JSON.stringify(data || {folders: []}), {
        headers: {'Content-Type': 'application/json', ...corsHeaders}
      });
    }

    // PUT — save full pipeline (overwrite)
    if (context.request.method === 'PUT') {
      const body = await context.request.json();
      await KV.put('pipeline', JSON.stringify(body));
      return new Response(JSON.stringify({ok: true}), {
        headers: {'Content-Type': 'application/json', ...corsHeaders}
      });
    }

    return new Response(JSON.stringify({error: 'method not allowed'}), {
      status: 405, headers: {'Content-Type': 'application/json', ...corsHeaders}
    });

  } catch (e) {
    return new Response(JSON.stringify({error: e.message}), {
      status: 500, headers: {'Content-Type': 'application/json', ...corsHeaders}
    });
  }
}
