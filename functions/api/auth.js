export async function onRequest(context) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, GET',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (context.request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const url = new URL(context.request.url);
  const env = context.env;

  // --- Validate a key param (for cross-linking) ---
  if (context.request.method === 'GET') {
    const key = url.searchParams.get('key');
    if (!key) {
      return new Response(JSON.stringify({ valid: false }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const valid = await validatePassword(key, env);
    if (valid) {
      const token = await generateToken(env);
      return new Response(JSON.stringify({ valid: true, token }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
    return new Response(JSON.stringify({ valid: false }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  // --- Login with password (POST) ---
  if (context.request.method === 'POST') {
    try {
      const body = await context.request.json();
      const password = body.password;
      if (!password) {
        return new Response(JSON.stringify({ valid: false, error: 'no password' }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      const valid = await validatePassword(password, env);
      if (valid) {
        const token = await generateToken(env);
        return new Response(JSON.stringify({ valid: true, token }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      return new Response(JSON.stringify({ valid: false, error: 'invalid password' }), {
        status: 401, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (e) {
      return new Response(JSON.stringify({ valid: false, error: e.message }), {
        status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  return new Response('Method not allowed', { status: 405, headers: corsHeaders });
}

async function validatePassword(password, env) {
  // Check permanent password
  if (env.AUTH_PASSWORD && password === env.AUTH_PASSWORD) {
    return true;
  }

  // Check temp passwords
  if (env.TEMP_PASSWORDS) {
    try {
      const temps = JSON.parse(env.TEMP_PASSWORDS);
      const today = new Date().toISOString().split('T')[0];
      for (const t of temps) {
        if (password === t.password && today <= t.expires) {
          return true;
        }
      }
    } catch (e) { /* ignore parse errors */ }
  }

  return false;
}

async function generateToken(env) {
  const secret = env.AUTH_SECRET || 'fallback-secret';
  const payload = {
    exp: Date.now() + (24 * 60 * 60 * 1000), // 24 hours
    iat: Date.now()
  };
  // Simple HMAC-based token
  const data = JSON.stringify(payload);
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(data));
  const sigHex = [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
  return btoa(data) + '.' + sigHex;
}
