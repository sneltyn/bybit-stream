import WebSocket from 'ws'

import { BybitService } from '@apps/services/bybit.service'
import { BYBIT_PING_INTERVAL_MS, BYBIT_WS_URL, NATS_SUBJECT } from '@config/env'
import { logger } from '@config/logger'
import { nats } from '@config/nats'

// Bybit best bid/ask comes from the `tickers.<SYMBOL>` topic (linear),
// which carries absolute bid1Price/ask1Price fields and pushes at ~100ms
// on change — no orderbook delta/size-0 reconstruction needed.
const TICKER_TOPIC_PREFIX = 'tickers.'

const WS_BASE = BYBIT_WS_URL

const STALE_TIMEOUT_MS = 60_000
const STALE_CHECK_INTERVAL_MS = 15_000

// One topic per pair. Bybit public connections tolerate many topics; we
// still shard so a single dropped connection doesn't blank the whole feed.
const PAIRS_PER_CONNECTION = 100
// Bybit caps args per subscription request; keep batches small and spaced.
const SUBSCRIBE_BATCH_PARAMS = 10
const SUBSCRIBE_BATCH_INTERVAL_MS = 250

// Periodically re-fetch instruments-info so newly listed pairs are
// subscribed and delisted ones are dropped without a restart.
const INSTRUMENTS_REFRESH_INTERVAL_MS = 60 * 60 * 1_000

// Periodic visibility into stream health.
const HEARTBEAT_INTERVAL_MS = 30_000

// Liveness floor: republish each symbol's last bid/ask at least this
// often so downstream consumers don't see stale gaps. Driven by a
// standalone timer because Bybit can stop sending ticker updates for a
// symbol entirely when top-of-book is idle — we can't rely on incoming
// WS events to trigger the heartbeat.
const FORCE_REPUBLISH_MS = 500
const REPUBLISH_TICK_MS = 250

interface DedupState {
  lastBid: number
  lastAsk: number
  lastPublishAt: number
}

type MessageHandler = (raw: WebSocket.RawData) => void
type StreamOp = 'subscribe' | 'unsubscribe'

class ShardConnection {
  public readonly id: number
  private ws: WebSocket | null = null
  private retryCount = 0
  private retryTimer: NodeJS.Timeout | null = null
  private staleWatchdog: NodeJS.Timeout | null = null
  private pingTimer: NodeJS.Timeout | null = null
  private pendingTimers = new Set<NodeJS.Timeout>()
  private readonly pairs = new Set<string>()
  private lastMessageAt = 0
  private stopped = false

  public constructor(
    id: number,
    initialPairs: string[],
    private readonly onMessage: MessageHandler
  ) {
    this.id = id
    for (const p of initialPairs) this.pairs.add(p)
  }

  public start(): void {
    this.stopped = false
    this.retryCount = 0
    this.lastMessageAt = Date.now()
    this.connect()

    this.staleWatchdog = setInterval(() => {
      const idle = Date.now() - this.lastMessageAt
      if (idle > STALE_TIMEOUT_MS) {
        logger.warn(
          `[ws#${this.id}] stream idle for ${idle}ms, forcing reconnect`
        )
        this.lastMessageAt = Date.now()
        this.forceReconnect()
      }
    }, STALE_CHECK_INTERVAL_MS)
  }

  public stop(): void {
    this.stopped = true
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer)
      this.retryTimer = null
    }
    if (this.staleWatchdog !== null) {
      clearInterval(this.staleWatchdog)
      this.staleWatchdog = null
    }
    this.stopPing()
    this.clearPendingTimers()
    if (this.ws !== null) {
      try {
        this.ws.removeAllListeners()
        this.ws.close()
      } catch (err) {
        logger.error(`[ws#${this.id}] close error: ${String(err)}`)
      }
      this.ws = null
    }
  }

  public size(): number {
    return this.pairs.size
  }

  public hasPair(symbol: string): boolean {
    return this.pairs.has(symbol)
  }

  public idleMs(): number {
    return Date.now() - this.lastMessageAt
  }

  public isOpen(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN
  }

  public addPairs(toAdd: string[]): void {
    const added = toAdd.filter((p) => !this.pairs.has(p))
    for (const p of added) this.pairs.add(p)
    if (added.length === 0) return
    if (!this.isOpen()) return
    this.sendStreamRequest('subscribe', added)
  }

  public removePairs(toRemove: string[]): void {
    const removed = toRemove.filter((p) => this.pairs.has(p))
    for (const p of removed) this.pairs.delete(p)
    if (removed.length === 0) return
    if (!this.isOpen()) return
    this.sendStreamRequest('unsubscribe', removed)
  }

  private connect(): void {
    this.stopPing()
    logger.info(
      `[ws#${this.id}] connecting to ${WS_BASE} (pairs=${this.pairs.size})`
    )
    this.ws = new WebSocket(WS_BASE, { perMessageDeflate: false })

    this.ws.on('open', () => {
      logger.info(`[ws#${this.id}] connected`)
      this.retryCount = 0
      this.subscribeAll()
      this.startPing()
    })

    this.ws.on('message', (raw: WebSocket.RawData) => {
      this.lastMessageAt = Date.now()
      this.onMessage(raw)
    })

    this.ws.on('pong', () => {
      this.lastMessageAt = Date.now()
    })

    this.ws.on('close', (code, reason) => {
      this.stopPing()
      logger.warn(
        `[ws#${this.id}] disconnected code=${code} reason=${
          reason.toString() || 'n/a'
        }`
      )
      this.retry()
    })

    this.ws.on('error', (error) => {
      logger.error(`[ws#${this.id}] error: ${error.message}`)
    })
  }

  // Bybit closes idle public connections (~20s). Keep each shard alive
  // with an application-level {op:"ping"}; the pong returns as a control
  // frame and also refreshes lastMessageAt via the message handler.
  private startPing(): void {
    this.stopPing()
    this.pingTimer = setInterval(() => {
      if (!this.isOpen()) return
      try {
        this.ws?.send(
          JSON.stringify({ op: 'ping', req_id: `ping-${this.id}-${Date.now()}` })
        )
      } catch (err) {
        logger.error(`[ws#${this.id}] ping send failed: ${String(err)}`)
      }
    }, BYBIT_PING_INTERVAL_MS)
  }

  private stopPing(): void {
    if (this.pingTimer !== null) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
  }

  private forceReconnect(): void {
    if (this.stopped) return
    this.stopPing()
    if (this.ws !== null) {
      try {
        this.ws.removeAllListeners()
        this.ws.terminate()
      } catch {
        /* ignore */
      }
      this.ws = null
    }
    this.retry()
  }

  private retry(): void {
    if (this.stopped) return
    if (this.retryTimer !== null) return

    const baseDelay = Math.min(
      30_000,
      1_000 * 2 ** Math.min(this.retryCount, 5)
    )
    const jitter = Math.floor(Math.random() * 500)
    const delay = baseDelay + jitter

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null
      this.retryCount += 1
      logger.info(`[ws#${this.id}] retry attempt ${this.retryCount}`)

      this.stopPing()
      if (this.ws !== null) {
        try {
          this.ws.removeAllListeners()
          this.ws.close()
        } catch {
          /* ignore */
        }
        this.ws = null
      }

      this.connect()
    }, delay)
  }

  private subscribeAll(): void {
    this.clearPendingTimers()
    if (this.pairs.size === 0) {
      logger.warn(`[ws#${this.id}] no pairs assigned, skipping subscribe`)
      return
    }
    this.sendStreamRequest('subscribe', [...this.pairs])
  }

  private sendStreamRequest(op: StreamOp, pairs: string[]): void {
    if (pairs.length === 0) return

    const args = pairs.map((p) => `${TICKER_TOPIC_PREFIX}${p.toUpperCase()}`)
    const chunks: string[][] = []
    for (let i = 0; i < args.length; i += SUBSCRIBE_BATCH_PARAMS) {
      chunks.push(args.slice(i, i + SUBSCRIBE_BATCH_PARAMS))
    }

    logger.info(
      `[ws#${this.id}] ${op} pairs=${pairs.length} batches=${chunks.length}`
    )

    chunks.forEach((chunk, batchIdx) => {
      const delay = batchIdx * SUBSCRIBE_BATCH_INTERVAL_MS
      const timer: NodeJS.Timeout = setTimeout(() => {
        this.pendingTimers.delete(timer)
        if (this.ws === null || this.ws.readyState !== WebSocket.OPEN) return
        const msg = { op, args: chunk, req_id: `${op}-${this.id}-${batchIdx + 1}` }
        try {
          this.ws.send(JSON.stringify(msg))
        } catch (err) {
          logger.error(
            `[ws#${this.id}] ${op} send failed batch=${batchIdx}: ${String(err)}`
          )
        }
      }, delay)
      this.pendingTimers.add(timer)
    })
  }

  private clearPendingTimers(): void {
    for (const t of this.pendingTimers) clearTimeout(t)
    this.pendingTimers.clear()
  }
}

interface TickerData {
  symbol?: string
  bid1Price?: string
  ask1Price?: string
}

export class MarkPriceSocket {
  private static connections: ShardConnection[] = []
  private static heartbeatTimer: NodeJS.Timeout | null = null
  private static republishTimer: NodeJS.Timeout | null = null
  private static refreshTimer: NodeJS.Timeout | null = null
  private static nextShardId = 1
  private static ticksSinceHeartbeat = 0
  private static publishesSinceHeartbeat = 0
  private static dedupedSinceHeartbeat = 0
  private static controlSinceHeartbeat = 0
  private static otherSinceHeartbeat = 0
  private static unrecognizedSampleCount = 0
  private static refreshInFlight = false
  private static readonly lastPrices: Record<string, number> = {}
  private static readonly dedupStates = new Map<string, DedupState>()

  public static init(): void {
    const pairs = BybitService.pairs
    if (pairs.length === 0) {
      logger.warn('[ws] no pairs resolved from instruments-info, skipping init')
      return
    }

    const chunks: string[][] = []
    for (let i = 0; i < pairs.length; i += PAIRS_PER_CONNECTION) {
      chunks.push(pairs.slice(i, i + PAIRS_PER_CONNECTION))
    }

    logger.info(
      `[ws] sharding pairs=${pairs.length} across connections=${chunks.length} (cap ${PAIRS_PER_CONNECTION}/conn)`
    )

    this.connections = chunks.map(
      (slice) =>
        new ShardConnection(this.nextShardId++, slice, (raw) => {
          void this.handleMessage(raw)
        })
    )
    for (const c of this.connections) c.start()

    this.heartbeatTimer = setInterval(() => {
      this.logHeartbeat()
    }, HEARTBEAT_INTERVAL_MS)

    this.republishTimer = setInterval(() => {
      void this.republishStale()
    }, REPUBLISH_TICK_MS)

    this.refreshTimer = setInterval(() => {
      void this.refreshPairs()
    }, INSTRUMENTS_REFRESH_INTERVAL_MS)
  }

  public static async shutdown(): Promise<void> {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
    if (this.republishTimer !== null) {
      clearInterval(this.republishTimer)
      this.republishTimer = null
    }
    if (this.refreshTimer !== null) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = null
    }
    for (const c of this.connections) c.stop()
    this.connections = []
    this.dedupStates.clear()
  }

  private static async refreshPairs(): Promise<void> {
    if (this.refreshInFlight) return
    this.refreshInFlight = true
    try {
      const { added, removed } = await BybitService.refresh()
      if (added.length === 0 && removed.length === 0) {
        logger.debug('[refresh] no pair changes')
        return
      }
      logger.info(`[refresh] added=${added.length} removed=${removed.length}`)

      for (const sym of removed) {
        const shard = this.connections.find((c) => c.hasPair(sym))
        if (shard !== undefined) shard.removePairs([sym])
        delete this.lastPrices[sym]
        this.dedupStates.delete(sym)
      }

      for (const sym of added) {
        let shard = this.findLeastLoaded()
        if (shard === null || shard.size() >= PAIRS_PER_CONNECTION) {
          shard = new ShardConnection(this.nextShardId++, [sym], (raw) => {
            void this.handleMessage(raw)
          })
          this.connections.push(shard)
          shard.start()
          logger.info(
            `[refresh] spawned new shard #${shard.id} for ${sym} (total shards=${this.connections.length})`
          )
          continue
        }
        shard.addPairs([sym])
      }
    } catch (err) {
      logger.error(`[refresh] failed: ${String(err)}`)
    } finally {
      this.refreshInFlight = false
    }
  }

  private static findLeastLoaded(): ShardConnection | null {
    if (this.connections.length === 0) return null
    let best = this.connections[0]
    for (const c of this.connections) {
      if (c.size() < best.size()) best = c
    }
    return best
  }

  private static logHeartbeat(): void {
    const shards = this.connections.length
    const openShards = this.connections.filter((c) => c.isOpen()).length
    const totalPairs = this.connections.reduce((s, c) => s + c.size(), 0)
    const maxIdleMs = this.connections.reduce(
      (m, c) => Math.max(m, c.idleMs()),
      0
    )
    const ticks = this.ticksSinceHeartbeat
    const pubs = this.publishesSinceHeartbeat
    const dedup = this.dedupedSinceHeartbeat
    const ctrl = this.controlSinceHeartbeat
    const other = this.otherSinceHeartbeat
    this.ticksSinceHeartbeat = 0
    this.publishesSinceHeartbeat = 0
    this.dedupedSinceHeartbeat = 0
    this.controlSinceHeartbeat = 0
    this.otherSinceHeartbeat = 0

    const intervalSec = HEARTBEAT_INTERVAL_MS / 1_000
    logger.info(
      `[heartbeat] shards=${openShards}/${shards} pairs=${totalPairs} prices=${Object.keys(this.lastPrices).length} ticks/${intervalSec}s=${ticks} pubs/${intervalSec}s=${pubs} dedup/${intervalSec}s=${dedup} ctrl=${ctrl} other=${other} maxIdle=${(maxIdleMs / 1_000).toFixed(1)}s`
    )
  }

  private static async handleMessage(raw: WebSocket.RawData): Promise<void> {
    let parsed: unknown
    try {
      parsed = JSON.parse(raw.toString())
    } catch (err) {
      logger.error(`[ws] failed to parse message: ${String(err)}`)
      return
    }

    const events = Array.isArray(parsed) ? parsed : [parsed]

    for (const item of events as Array<Record<string, unknown>>) {
      // Control frames: subscribe/unsubscribe acks and ping/pong replies,
      // e.g. {success:true, op:'subscribe'} or {op:'pong'} /
      // {ret_msg:'pong', op:'ping'}. Anything carrying `op` (and no topic)
      // is a control frame, not market data.
      if (typeof item?.op === 'string' || item?.ret_msg === 'pong') {
        this.controlSinceHeartbeat += 1
        continue
      }

      const topic = item?.topic
      if (
        typeof topic === 'string' &&
        topic.startsWith(TICKER_TOPIC_PREFIX) &&
        item?.data !== null &&
        typeof item?.data === 'object'
      ) {
        this.ticksSinceHeartbeat += 1
        await this.processTicker(item)
        continue
      }

      this.otherSinceHeartbeat += 1
      if (this.unrecognizedSampleCount < 3) {
        this.unrecognizedSampleCount += 1
        logger.warn(
          `[ws] unrecognized frame sample #${this.unrecognizedSampleCount}: ${JSON.stringify(item).slice(0, 300)}`
        )
      }
    }
  }

  private static async processTicker(
    item: Record<string, unknown>
  ): Promise<void> {
    const data = item.data as TickerData
    const symbol = typeof data.symbol === 'string' ? data.symbol : ''
    if (symbol.length === 0) return

    const tickSize = BybitService.tickSizes[symbol]

    let state = this.dedupStates.get(symbol)
    if (state === undefined) {
      state = { lastBid: NaN, lastAsk: NaN, lastPublishAt: 0 }
      this.dedupStates.set(symbol, state)
    }

    // `tickers` deltas carry only the fields that changed, so a frame may
    // update just one side. Start from the last known bid/ask and overlay
    // whatever this frame provides; the snapshot (sent first on subscribe)
    // seeds both sides.
    let bid = state.lastBid
    let ask = state.lastAsk

    if (data.bid1Price !== undefined) {
      const b = Number(data.bid1Price)
      if (Number.isFinite(b) && b > 0) bid = this.roundPriceToTickSize(b, tickSize)
    }
    if (data.ask1Price !== undefined) {
      const a = Number(data.ask1Price)
      if (Number.isFinite(a) && a > 0) ask = this.roundPriceToTickSize(a, tickSize)
    }

    if (!Number.isFinite(bid) || !Number.isFinite(ask)) return

    const unchanged = bid === state.lastBid && ask === state.lastAsk

    // Only changes publish here. Unchanged values are republished by the
    // standalone heartbeat timer (republishStale), which guarantees a
    // publish at least every FORCE_REPUBLISH_MS even if no WS events
    // arrive for the symbol.
    if (unchanged) {
      this.dedupedSinceHeartbeat += 1
      return
    }

    state.lastBid = bid
    state.lastAsk = ask
    const price = this.roundPriceToTickSize((bid + ask) / 2, tickSize)
    this.lastPrices[symbol] = price

    const timestamp = Number(item.ts) || Date.now()
    await this.publish(symbol, bid, ask, price, timestamp, state)
  }

  private static async publish(
    symbol: string,
    bid: number,
    ask: number,
    price: number,
    timestamp: number,
    state: DedupState
  ): Promise<void> {
    try {
      // Single coarse subject; raw Bybit `symbol` in the payload. coins
      // maps Bybit→canonical and resolves category downstream.
      const payload = { symbol, timestamp, price, bid, ask }
      // No msgID — JetStream MsgID dedup (duplicate_window=2min) would
      // silently drop heartbeat republishes and any change publishes that
      // happen to share an updateId, starving the downstream consumer.
      await nats.publishJSON(NATS_SUBJECT, payload)
      state.lastPublishAt = Date.now()
      this.publishesSinceHeartbeat += 1
    } catch (err) {
      logger.error(
        `[nats] publish failed symbol=${symbol} ts=${timestamp}: ${String(err)}`
      )
    }
  }

  private static async republishStale(): Promise<void> {
    const now = Date.now()
    const due: Array<[string, DedupState]> = []
    for (const [symbol, state] of this.dedupStates) {
      if (!Number.isFinite(state.lastBid) || !Number.isFinite(state.lastAsk)) {
        continue
      }
      if (now - state.lastPublishAt < FORCE_REPUBLISH_MS) continue
      due.push([symbol, state])
    }
    if (due.length === 0) return

    await Promise.all(
      due.map(([symbol, state]) => {
        const tickSize = BybitService.tickSizes[symbol]
        const price = this.roundPriceToTickSize(
          (state.lastBid + state.lastAsk) / 2,
          tickSize
        )
        return this.publish(
          symbol,
          state.lastBid,
          state.lastAsk,
          price,
          now,
          state
        )
      })
    )
  }

  private static roundPriceToTickSize(
    price: number,
    tickSize?: string
  ): number {
    if (!tickSize) return price

    const tick = parseFloat(tickSize)
    if (!Number.isFinite(tick) || tick === 0) return price

    const dotIdx = tickSize.indexOf('.')
    const decimals = dotIdx === -1 ? 0 : tickSize.length - dotIdx - 1

    // Floor-to-tick with a relative-epsilon nudge to absorb FP error
    // in `price / tick`. Example: 80621.7 / 0.1 yields 806216.999… in
    // JS, so a naive floor returns 806216 — shifting the rounded value
    // down by a full tick. The 1e-12 nudge is well above FP noise
    // (~2e-16 relative) and well below any meaningful tick fraction.
    const ratio = price / tick
    const n = Math.floor(ratio + Math.abs(ratio) * 1e-12)
    return Number((n * tick).toFixed(decimals))
  }
}
