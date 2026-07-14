# Painel Vercel

Esta pasta é a parte que vai para a Vercel.

A Vercel hospeda:

- painel da equipe;
- domínio;
- HTTPS;
- interface pública/protegida.

A Vercel **não** roda:

- worker de rádio 24h;
- ffmpeg contínuo;
- transcrição contínua;
- banco persistente principal.

Essas partes ficam no backend Docker (`../docker-compose.yml`) em uma VPS/Render/Railway/Fly.

## Configurar API

No painel da Vercel, configure a variável de ambiente:

```text
RADAR_API_URL=https://api-radar.seudominio.com.br
```

Isso gera automaticamente o `public/config.js` no build.

Também dá para editar `public/config.js` manualmente:

```js
window.RADAR_API_URL = "https://api-radar.seudominio.com.br";
```

Se o painel e API estiverem no mesmo domínio, deixe vazio.
