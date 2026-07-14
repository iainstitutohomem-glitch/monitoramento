# Radar Online 24h

Esta pasta é a base de produção do Radar de Imprensa. Ela separa o protótipo local em três partes:

- `api`: painel online, API, banco e arquivos de áudio.
- `radio-worker`: processo 24h que escuta rádios, transcreve e salva achados.
- `online-worker`: processo 24h que busca notícias/resultados online por termos.

## Como rodar em servidor

Em uma VPS/servidor com Docker:

```bash
docker compose up -d --build
```

Depois acesse:

```text
http://IP_DO_SERVIDOR:8000
```

Em produção, coloque um domínio por cima, por exemplo:

```text
https://radar.institutohomem.com.br
```

## O que já entra pronto

- 65 rádios/mídias importadas da lista IH.
- Termos iniciais.
- Painel online para equipe.
- Banco persistente em `storage/radar.db`.
- Worker de rádio 24h.
- Worker de busca online 24h.
- Storage local de áudios em `storage/audio`.
- Docker Compose para rodar tudo sempre ligado.

## Como escalar rádio

Cada `radio-worker` monitora `RADAR_CONCURRENCY` rádios ao mesmo tempo.

Exemplo:

- `RADAR_CONCURRENCY=4`: 4 rádios simultâneas.
- Para 40 rádios, rode 10 workers de 4 rádios ou aumente conforme CPU/memória.

Em produção, o ideal é começar com:

- 1 servidor para API/painel/banco.
- 2 a 4 workers de rádio.
- 1 worker de busca online.
- Storage externo para áudios quando o volume crescer.

## Próximo passo de produção

Escolher onde hospedar:

- VPS: Hetzner, DigitalOcean, AWS Lightsail, Azure VM.
- Plataforma Docker: Render, Railway, Fly.io.
- Banco gerenciado: Postgres.
- Storage de áudio: S3, Cloudflare R2 ou Supabase Storage.

Para 24h real, não use notebook. Use servidor sempre ligado.
