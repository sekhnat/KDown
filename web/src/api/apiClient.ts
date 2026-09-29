/** Shared singleton API client; bootstrap supplies the CSRF token. */
import { ApiClient } from './client'

export const api = new ApiClient()
export { ApiError } from './client'
export type { ConflictPolicy, ArtifactPolicy, JobView, Bootstrap, JobPage, JobDetail } from './client'
