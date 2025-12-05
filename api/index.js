// api/index.js (Vercel Serverless Function)
// projeto está com "type": "module", então usamos import/export ESModule

import handler from '../serverless.js';

export default function (req, res) {
  return handler(req, res);
}
