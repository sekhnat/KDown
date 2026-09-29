/**
 * Process harness: owns temporary state/download directories and the child
 * processes used by one browser scenario. Fixture startup parses the complete
 * LISTENING/SIZE/SHA256/READY protocol; app restarts preserve the original
 * loopback address so the browser can reconnect to the same origin.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { createHash } from 'node:crypto'
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
} from 'node:fs'
import { createInterface } from 'node:readline'
import { tmpdir } from 'node:os'
import path from 'node:path'

const KDOWN_BIN = process.env.KDOWN_BIN ?? 'target/release/kdown-app'
const FIXTURE_BIN = process.env.FIXTURE_BIN ?? 'target/debug/fixture_server'
const WEB_DIR = process.env.KDOWN_WEB_DIR ?? path.resolve('web', 'dist')

export interface TestProcesses {
  readonly kdownUrl: string
  readonly fixtureUrl: string
  readonly slowFixtureUrl: string
  readonly downloadRoot: string
  failingFixtureUrl(): Promise<string>
  restartApp(whileStopped?: () => Promise<void>): Promise<void>
  expectDownloadedSha256(filename: string, fixture?: 'fast' | 'slow'): Promise<void>
  listDownloadRoot(): string[]
  stop(): Promise<void>
}

/** One readline per stream feeding ordered matchers. */
function lineWatcher(stream: NodeJS.ReadableStream) {
  type Waiter = {
    match: RegExp
    label: string
    resolve(value: string): void
    reject(error: Error): void
    timer: NodeJS.Timeout
  }
  const waiters: Waiter[] = []
  const lines = createInterface({ input: stream })
  lines.on('line', (line) => {
    for (let index = 0; index < waiters.length; index += 1) {
      const waiter = waiters[index]
      if (waiter.match.test(line)) {
        waiters.splice(index, 1)
        clearTimeout(waiter.timer)
        waiter.resolve(line)
        break
      }
    }
  })
  lines.on('close', () => {
    for (const waiter of waiters.splice(0)) {
      clearTimeout(waiter.timer)
      waiter.reject(new Error(`stream closed before ${waiter.label}`))
    }
  })
  return (match: RegExp, label: string, timeoutMs = 30_000): Promise<string> => {
    const { promise, resolve, reject } = Promise.withResolvers<string>()
    const waiter: Waiter = {
      match,
      label,
      resolve,
      reject,
      timer: setTimeout(() => {
        const position = waiters.findIndex((candidate) => candidate.label === label)
        if (position >= 0) waiters.splice(position, 1)
        reject(new Error(`timeout waiting for ${label} (${match})`))
      }, timeoutMs),
    }
    waiters.push(waiter)
    return promise
  }
}

interface FixtureProcess {
  child: ChildProcess
  url: string
  sha256: string
}

async function startFixture(args: string[]): Promise<FixtureProcess> {
  const child = spawn(FIXTURE_BIN, ['--addr', '127.0.0.1:0', ...args])
  // Register every matcher before awaiting: lines may arrive in one chunk
  // between registration points.
  const watch = lineWatcher(child.stdout!)
  const listeningPromise = watch(/^LISTENING /, 'LISTENING')
  const sizePromise = watch(/^SIZE /, 'SIZE')
  const shaPromise = watch(/^SHA256 /, 'SHA256')
  const readyPromise = watch(/^READY$/, 'READY')
  const [listening, , shaLine] = await Promise.all([
    listeningPromise,
    sizePromise,
    shaPromise,
    readyPromise,
  ])
  const addr = listening.replace('LISTENING ', '').trim()
  return { child, url: `http://${addr}`, sha256: shaLine.replace('SHA256 ', '').trim() }
}

export async function startTestProcesses(): Promise<TestProcesses> {
  const stateDir = mkdtempSync(path.join(tmpdir(), 'kdown-state-'))
  const downloadRoot = path.join(mkdtempSync(path.join(tmpdir(), 'kdown-down-')), 'downloads')
  mkdirSync(downloadRoot, { recursive: true })

  const fixture = await startFixture([
    '--size',
    String(2 * 1024 * 1024),
    '--seed',
    '7',
    '--throttle-mib-s',
    '32',
  ])
  const slowFixture = await startFixture([
    '--size',
    String(32 * 1024 * 1024),
    '--seed',
    '11',
    '--throttle-mib-s',
    '1',
  ])
  let failingFixture: FixtureProcess | null = null

  let app: ChildProcess | null = null
  let readyUrl = ''
  let listenAddress = '127.0.0.1:0'

  async function startApp(): Promise<void> {
    app = spawn(KDOWN_BIN, [
      'serve',
      '--listen',
      listenAddress,
      '--state-dir',
      stateDir,
      '--web-dir',
      WEB_DIR,
      '--root',
      downloadRoot,
    ])
    const watch = lineWatcher(app.stdout!)
    const ready = await watch(/^READY /, 'app READY')
    readyUrl = ready.replace('READY ', '').trim()
    listenAddress = new URL(readyUrl).host
  }

  async function stopApp(): Promise<void> {
    if (app && app.exitCode === null) {
      app.kill('SIGTERM')
      const { promise, resolve } = Promise.withResolvers<void>()
      const timer = setTimeout(() => {
        app?.kill('SIGKILL')
        resolve()
      }, 5000)
      app.once('exit', () => {
        clearTimeout(timer)
        resolve()
      })
      await promise
    }
    app = null
  }

  await startApp()

  const processes: TestProcesses = {
    kdownUrl: readyUrl,
    fixtureUrl: fixture.url,
    slowFixtureUrl: slowFixture.url,
    downloadRoot,
    async failingFixtureUrl() {
      failingFixture ??= await startFixture([
        '--size',
        String(1024 * 1024),
        '--seed',
        '3',
        '--transient-fail',
        '100000:404',
      ])
      return failingFixture.url
    },
    async restartApp(whileStopped) {
      await stopApp()
      await whileStopped?.()
      await startApp()
    },
    async expectDownloadedSha256(filename, fixtureKind = 'fast') {
      const target = path.join(downloadRoot, filename)
      const deadline = Date.now() + 90_000
      while (!existsSync(target)) {
        if (Date.now() > deadline) {
          const listing = existsSync(downloadRoot) ? readdirSync(downloadRoot) : []
          throw new Error(
            `downloaded file never appeared: ${target}; root listing: ${JSON.stringify(listing)}`,
          )
        }
        const { promise, resolve } = Promise.withResolvers<void>()
        setTimeout(resolve, 500)
        await promise
      }
      const digest = createHash('sha256').update(readFileSync(target)).digest('hex')
      const expected = fixtureKind === 'slow' ? slowFixture.sha256 : fixture.sha256
      if (digest !== expected) {
        throw new Error(`sha256 mismatch: disk=${digest} expected=${expected}`)
      }
    },
    listDownloadRoot() {
      return existsSync(downloadRoot) ? readdirSync(downloadRoot) : []
    },
    async stop() {
      await stopApp()
      fixture.child.kill('SIGKILL')
      slowFixture.child.kill('SIGKILL')
      failingFixture?.child.kill('SIGKILL')
      rmSync(stateDir, { recursive: true, force: true })
      rmSync(path.dirname(downloadRoot), { recursive: true, force: true })
    },
  }

  return processes
}
