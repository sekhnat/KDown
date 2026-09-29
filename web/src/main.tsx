import { createRoot } from 'react-dom/client'
import { App } from './app/App'
import './styles/global.css'

const container = document.getElementById('root')
if (!container) {
  throw new Error('missing #root container')
}

createRoot(container).render(<App />)
