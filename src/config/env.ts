import 'dotenv/config'
import * as fs from 'fs'
import * as path from 'path'
import * as process from 'process'

// node env
export const NODE_ENV = process.env.NODE_ENV ?? 'development'

export const APP_NAME = process.env.APP_NAME ?? process.env.npm_package_name
export const APP_PORT = process.env.APP_PORT
  ? Number(process.env.APP_PORT)
  : 11000

// nats
export const NATS_SERVERS = (
  process.env.NATS_SERVERS ?? 'nats://127.0.0.1:4222'
)
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0)

export const NATS_NAME = process.env.NATS_NAME ?? APP_NAME ?? 'bybit-stream'
export const NATS_USER = process.env.NATS_USER ?? undefined
export const NATS_PASSWORD = process.env.NATS_PASSWORD ?? undefined
export const NATS_TOKEN = process.env.NATS_TOKEN ?? undefined
export const NATS_CREDS_FILE = process.env.NATS_CREDS_FILE ?? undefined

export const NATS_RECONNECT_WAIT_MS = process.env.NATS_RECONNECT_WAIT_MS
  ? Number(process.env.NATS_RECONNECT_WAIT_MS)
  : 2_000

export const NATS_RECONNECT_JITTER_MS = process.env.NATS_RECONNECT_JITTER_MS
  ? Number(process.env.NATS_RECONNECT_JITTER_MS)
  : 500

export const NATS_PUBLISH_TIMEOUT_MS = process.env.NATS_PUBLISH_TIMEOUT_MS
  ? Number(process.env.NATS_PUBLISH_TIMEOUT_MS)
  : 5_000

// nats jetstream
//
// bybit-stream publishes every tick to ONE coarse subject and does NOT
// split by category or normalize symbols — `payload.symbol` carries the
// RAW Bybit symbol (e.g. SHIB1000USDT). coins owns Bybit→canonical
// mapping and category resolution (single translation boundary).
export const NATS_SUBJECT = process.env.NATS_SUBJECT ?? 'prices.bybit'

export const NATS_STREAM_NAME = process.env.NATS_STREAM_NAME ?? 'PRICES'

export const NATS_STREAM_SUBJECTS = (
  process.env.NATS_STREAM_SUBJECTS ?? 'prices.>'
)
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0)

export const NATS_STREAM_MAX_AGE_MS = process.env.NATS_STREAM_MAX_AGE_MS
  ? Number(process.env.NATS_STREAM_MAX_AGE_MS)
  : 5 * 60_000 // 5 minutes

export const NATS_STREAM_MAX_BYTES = process.env.NATS_STREAM_MAX_BYTES
  ? Number(process.env.NATS_STREAM_MAX_BYTES)
  : 1024 * 1024 * 1024 // 1 GiB

export const NATS_STREAM_REPLICAS = process.env.NATS_STREAM_REPLICAS
  ? Number(process.env.NATS_STREAM_REPLICAS)
  : 1

// bybit
export const BYBIT_WS_URL =
  process.env.BYBIT_WS_URL ?? 'wss://stream.bybit.com/v5/public/linear'

export const BYBIT_INSTRUMENTS_URL =
  process.env.BYBIT_INSTRUMENTS_URL ??
  'https://api.bybit.com/v5/market/instruments-info'

export const BYBIT_CATEGORY = process.env.BYBIT_CATEGORY ?? 'linear'

// Bybit closes idle public connections (~20s). An app-level {op:"ping"}
// keeps each shard alive; pong arrives back as a control frame.
export const BYBIT_PING_INTERVAL_MS = process.env.BYBIT_PING_INTERVAL_MS
  ? Number(process.env.BYBIT_PING_INTERVAL_MS)
  : 20_000

export const INSTANCE_ID = process.env.NODE_APP_INSTANCE
  ? Number(process.env.NODE_APP_INSTANCE)
  : 0

export const MAX_INSTANCE_COUNT = process.env.MAX_INSTANCE_COUNT
  ? Number(process.env.MAX_INSTANCE_COUNT)
  : 3

function loadPairs(): string[] {
  const configPath =
    process.env.PAIRS_CONFIG ?? path.resolve(process.cwd(), 'pairs.json')
  if (fs.existsSync(configPath)) {
    return JSON.parse(fs.readFileSync(configPath, 'utf-8'))
  }
  if (process.env.PAIRS) {
    return process.env.PAIRS.split(',')
  }
  return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
}

export const PAIRS = loadPairs()
