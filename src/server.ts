import 'module-alias/register'
import './pathAlias'

import { NODE_ENV } from '@config/env'
import { logger } from '@config/logger'
import { nats } from '@config/nats'

import { BybitService } from '@apps/services/bybit.service'
import { MarkPriceSocket } from '@apps/sockets/mark-price-socket'

logger.info(`NODE_ENV=${NODE_ENV}`)

let shuttingDown = false

async function shutdown(signal: string, code = 0): Promise<void> {
  if (shuttingDown) return
  shuttingDown = true
  logger.info(`[shutdown] received ${signal}`)

  const timeout = setTimeout(() => {
    logger.error('[shutdown] timeout exceeded, forcing exit')
    process.exit(1)
  }, 10_000)
  timeout.unref()

  try {
    await MarkPriceSocket.shutdown()
    await nats.drain()
  } catch (err) {
    logger.error(`[shutdown] error: ${String(err)}`)
    code = 1
  } finally {
    clearTimeout(timeout)
    process.exit(code)
  }
}

async function run(): Promise<void> {
  nats.onUnexpectedClose((err) => {
    logger.error(
      `[nats] connection lost permanently, exiting for supervisor restart: ${String(err)}`
    )
    void shutdown('nats-closed', 1)
  })
  await nats.connect()
  await BybitService.init()
  MarkPriceSocket.init()
}

process.on('SIGINT', () => {
  void shutdown('SIGINT')
})
process.on('SIGTERM', () => {
  void shutdown('SIGTERM')
})
process.on('uncaughtException', (err) => {
  logger.error(`[uncaughtException] ${err.stack ?? err.message}`)
  void shutdown('uncaughtException', 1)
})
process.on('unhandledRejection', (reason) => {
  logger.error(`[unhandledRejection] ${String(reason)}`)
})

void run().catch((err) => {
  logger.error(`[boot] failed to start: ${String(err)}`)
  void shutdown('boot-failure', 1)
})
