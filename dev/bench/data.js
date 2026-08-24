window.BENCHMARK_DATA = {
  "lastUpdate": 1787559277744,
  "repoUrl": "https://github.com/hseeberger/tellus",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "git@heikoseeberger.de",
            "name": "Heiko Seeberger",
            "username": "hseeberger"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "81abe1b9e52d101b82d569e560c4b4d1c2c4e20b",
          "message": "Merge pull request #34 from hseeberger/chore/rename-to-tellus\n\nchore: rename ferrier to tellus",
          "timestamp": "2026-08-24T10:12:41+02:00",
          "tree_id": "91fea01e3f4f3bb141e327ad3f77642f84571f1b",
          "url": "https://github.com/hseeberger/tellus/commit/81abe1b9e52d101b82d569e560c4b4d1c2c4e20b"
        },
        "date": 1787559277060,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 21455545,
            "range": "± 2238763",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 26459527,
            "range": "± 730594",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 673480,
            "range": "± 3174",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 800771,
            "range": "± 13549",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 8506098,
            "range": "± 223654",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 6133440,
            "range": "± 256620",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}