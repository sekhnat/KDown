import { afterAll, afterEach, beforeAll } from 'vitest'
import { server } from './msw'

// Feature tests manage the server themselves; this setup file exists for
// suites that want the default handlers without declaring them.
beforeAll(() => server.listen())
afterEach(() => server.resetHandlers())
afterAll(() => server.close())
