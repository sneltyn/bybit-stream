import { BYBIT_CATEGORY, BYBIT_INSTRUMENTS_URL, PAIRS } from '@config/env'
import { logger } from '@config/logger'

const INSTRUMENTS_TIMEOUT_MS = 10_000
const INIT_RETRY_MAX_DELAY_MS = 30_000
const INSTRUMENTS_PAGE_LIMIT = 1_000
// Defensive cap so a misbehaving nextPageCursor can never loop forever.
const MAX_PAGES = 10

interface BybitInstrument {
  symbol: string
  status?: string
  contractType?: string
  priceFilter?: { tickSize?: string }
}

interface InstrumentsResponse {
  retCode?: number
  retMsg?: string
  result?: {
    list?: BybitInstrument[]
    nextPageCursor?: string
  }
}

export interface InstrumentsDiff {
  added: string[]
  removed: string[]
}

export class BybitService {
  public static pairs: string[] = []
  public static tickSizes: Record<string, string> = {}

  public static async init(): Promise<void> {
    let attempt = 0
    while (true) {
      try {
        await BybitService.loadInstruments()
        return
      } catch (error) {
        attempt += 1
        const baseDelay = Math.min(
          INIT_RETRY_MAX_DELAY_MS,
          1_000 * 2 ** Math.min(attempt - 1, 5)
        )
        const jitter = Math.floor(Math.random() * 500)
        const delay = baseDelay + jitter
        logger.error(
          `[bybit] init attempt ${attempt} failed: ${String(error)} — retry in ${delay}ms`
        )
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }
  }

  public static async refresh(): Promise<InstrumentsDiff> {
    const before = new Set(this.pairs)
    await this.loadInstruments()
    const after = new Set(this.pairs)
    const added = [...after].filter((p) => !before.has(p))
    const removed = [...before].filter((p) => !after.has(p))
    return { added, removed }
  }

  private static async loadInstruments(): Promise<void> {
    const instruments: BybitInstrument[] = []
    let cursor = ''

    for (let page = 0; page < MAX_PAGES; page += 1) {
      const params = new URLSearchParams({
        category: BYBIT_CATEGORY,
        limit: String(INSTRUMENTS_PAGE_LIMIT),
      })
      if (cursor) params.set('cursor', cursor)

      const res = await fetch(`${BYBIT_INSTRUMENTS_URL}?${params.toString()}`, {
        signal: AbortSignal.timeout(INSTRUMENTS_TIMEOUT_MS),
      })
      if (!res.ok) {
        throw new Error(`instruments-info HTTP ${res.status}`)
      }
      const body = (await res.json()) as InstrumentsResponse
      if (body.retCode !== 0) {
        throw new Error(
          `instruments-info retCode=${body.retCode} ${body.retMsg ?? ''}`
        )
      }

      instruments.push(...(body.result?.list ?? []))
      cursor = body.result?.nextPageCursor ?? ''
      if (!cursor) break
    }

    // Keep only tradable perpetuals. category=linear also returns dated
    // LinearFutures contracts (BTC-26DEC25 etc.); restrict to perpetuals so
    // they don't leak into NATS as standalone pairs.
    const tradablePerpetuals = instruments.filter(
      (s) => s.contractType === 'LinearPerpetual' && s.status === 'Trading'
    )

    const selectedPairs =
      PAIRS.length === 0
        ? tradablePerpetuals
        : tradablePerpetuals.filter((pair) => PAIRS.includes(pair.symbol))

    // Rebuild from scratch so delisted symbols disappear from the maps.
    const tickSizes: Record<string, string> = {}
    for (const pair of selectedPairs) {
      if (pair.priceFilter?.tickSize !== undefined) {
        tickSizes[pair.symbol] = pair.priceFilter.tickSize
      }
    }

    this.tickSizes = tickSizes
    this.pairs = selectedPairs
      .map((p) => p.symbol)
      .sort((a, b) => a.localeCompare(b))

    logger.info(
      `[bybit] pairs=${this.pairs.length} (PAIRS config ${
        PAIRS.length === 0 ? 'empty → all linear perpetuals' : `= ${PAIRS.length}`
      })`
    )
  }
}

export const bybitService = new BybitService()
