// Custom Layers API — stores GeoJSON layers in KV for persistence
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

  const url = new URL(context.request.url);
  const key = url.searchParams.get('key');
  if (key !== context.env.AUTH_PASSWORD) {
    return new Response(JSON.stringify({error: 'unauthorized'}), {
      status: 401, headers: {'Content-Type': 'application/json', ...corsHeaders}
    });
  }

  try {
    if (context.request.method === 'GET') {
      const data = await KV.get('custom_layers', 'json');
      return new Response(JSON.stringify(data || {layers: []}), {
        headers: {'Content-Type': 'application/json', ...corsHeaders}
      });
    }

    if (context.request.method === 'PUT') {
      const body = await context.request.json();
      await KV.put('custom_layers', JSON.stringify(body));
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
