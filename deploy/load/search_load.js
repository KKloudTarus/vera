// k6 load test for the memory search read path.
//
//   BASE_URL=https://vera.example VERA_API_KEY=vera_xxx.yyy k6 run search_load.js
//
// SLOs (thresholds below): p95 search latency < 800ms, error rate < 1%.
import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const API_KEY = __ENV.VERA_API_KEY || "";

const QUERIES = [
  "what runs on the prod cluster",
  "which team owns the payment service",
  "what does the checkout service depend on",
  "recent incidents and their causes",
  "how is engagement scope enforced",
];

export const options = {
  stages: [
    { duration: "30s", target: 20 }, // ramp up
    { duration: "2m", target: 20 }, // sustain
    { duration: "30s", target: 0 }, // ramp down
  ],
  thresholds: {
    http_req_duration: ["p(95)<800"],
    http_req_failed: ["rate<0.01"],
  },
};

export default function () {
  const q = QUERIES[Math.floor(Math.random() * QUERIES.length)];
  const res = http.post(
    `${BASE_URL}/memory/search`,
    JSON.stringify({ text: q, limit: 10 }),
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${API_KEY}`,
      },
    },
  );
  check(res, {
    "status is 200": (r) => r.status === 200,
    "returns a list": (r) => Array.isArray(r.json()),
  });
  sleep(1);
}
