import { readFileSync } from 'fs'
import {
  connect,
  credsAuthenticator,
  DiscardPolicy,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
  type PubAck,
  RetentionPolicy,
  StorageType,
  StringCodec,
} from 'nats'

import {
  NATS_CREDS_FILE,
  NATS_NAME,
  NATS_PASSWORD,
  NATS_PUBLISH_TIMEOUT_MS,
  NATS_RECONNECT_JITTER_MS,
  NATS_RECONNECT_WAIT_MS,
  NATS_SERVERS,
  NATS_STREAM_MAX_AGE_MS,
  NATS_STREAM_MAX_BYTES,
  NATS_STREAM_NAME,
  NATS_STREAM_REPLICAS,
  NATS_STREAM_SUBJECTS,
  NATS_TOKEN,
  NATS_USER,
} from './env'
import { logger } from './logger'

const sc = StringCodec()

type UnexpectedCloseHandler = (err?: unknown) => void

class NatsClient {
  private nc: NatsConnection | null = null
  private js: JetStreamClient | null = null
  private jsm: JetStreamManager | null = null
  private connecting: Promise<void> | null = null
  private closed = false
  private unexpectedCloseHandlers: UnexpectedCloseHandler[] = []

  public onUnexpectedClose(handler: UnexpectedCloseHandler): void {
    this.unexpectedCloseHandlers.push(handler)
  }

  public async connect(): Promise<void> {
    if (this.nc !== null) return
    if (this.connecting !== null) {
      await this.connecting
      return
    }

    this.connecting = this.doConnect()
    try {
      await this.connecting
    } finally {
      this.connecting = null
    }
  }

  private async doConnect(): Promise<void> {
    const authenticator =
      NATS_CREDS_FILE !== undefined
        ? credsAuthenticator(readFileSync(NATS_CREDS_FILE))
        : undefined

    this.nc = await connect({
      servers: NATS_SERVERS,
      name: NATS_NAME,
      user: NATS_USER,
      pass: NATS_PASSWORD,
      token: NATS_TOKEN,
      authenticator,
      maxReconnectAttempts: -1,
      reconnectTimeWait: NATS_RECONNECT_WAIT_MS,
      reconnectJitter: NATS_RECONNECT_JITTER_MS,
      pingInterval: 30_000,
      maxPingOut: 3,
      waitOnFirstConnect: true,
      noEcho: true,
    })

    this.observeStatus(this.nc)
    this.jsm = await this.nc.jetstreamManager()
    this.js = this.nc.jetstream()
    await this.ensureStream()

    logger.info(`[nats] connected to ${this.nc.getServer()}`)
  }

  private async ensureStream(): Promise<void> {
    if (this.jsm === null) {
      throw new Error('JetStreamManager is not initialized')
    }

    const config = {
      name: NATS_STREAM_NAME,
      subjects: NATS_STREAM_SUBJECTS,
      retention: RetentionPolicy.Limits,
      storage: StorageType.File,
      discard: DiscardPolicy.Old,
      max_age: NATS_STREAM_MAX_AGE_MS * 1_000_000, // ms -> ns
      max_bytes: NATS_STREAM_MAX_BYTES,
      num_replicas: NATS_STREAM_REPLICAS,
      duplicate_window: 2 * 60 * 1_000_000_000, // 2 minutes in ns
    }

    try {
      await this.jsm.streams.info(NATS_STREAM_NAME)
      await this.jsm.streams.update(NATS_STREAM_NAME, config)
      logger.info(`[nats] stream "${NATS_STREAM_NAME}" updated`)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      if (/not found|10059/i.test(message)) {
        await this.jsm.streams.add(config)
        logger.info(`[nats] stream "${NATS_STREAM_NAME}" created`)
        return
      }
      throw err
    }
  }

  private observeStatus(nc: NatsConnection): void {
    void (async () => {
      for await (const s of nc.status()) {
        const data = typeof s.data === 'string' ? s.data : ''
        logger.info(`[nats] status=${s.type}${data ? ` ${data}` : ''}`)
      }
    })().catch((err) => {
      logger.error(`[nats] status iterator error: ${String(err)}`)
    })

    void nc.closed().then((err) => {
      const wasIntentional = this.closed
      this.nc = null
      this.js = null
      this.jsm = null
      if (err !== undefined) {
        logger.error(`[nats] connection closed with error: ${String(err)}`)
      } else {
        logger.info('[nats] connection closed')
      }
      if (!wasIntentional) {
        for (const h of this.unexpectedCloseHandlers) {
          try {
            h(err)
          } catch (e) {
            logger.error(`[nats] close handler threw: ${String(e)}`)
          }
        }
      }
    })
  }

  public async publishJSON(
    subject: string,
    payload: unknown,
    msgID?: string
  ): Promise<PubAck> {
    if (this.js === null) {
      throw new Error('NATS JetStream client is not connected')
    }

    return await this.js.publish(subject, sc.encode(JSON.stringify(payload)), {
      timeout: NATS_PUBLISH_TIMEOUT_MS,
      msgID,
    })
  }

  public async drain(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (this.nc === null) return
    try {
      await this.nc.drain()
    } catch (err) {
      logger.error(`[nats] drain error: ${String(err)}`)
    }
  }

  public isConnected(): boolean {
    return this.nc !== null && !this.nc.isClosed()
  }
}

export const nats = new NatsClient()
