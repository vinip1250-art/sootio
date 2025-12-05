// api/index.js - Vercel Serverless Function

import handler from '../serverless.js';

export default function (req, res) {
  return handler(req, res);
}
