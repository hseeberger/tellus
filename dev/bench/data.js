window.BENCHMARK_DATA = {
  "lastUpdate": 1787724927961,
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
      },
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
          "id": "e816039a5bd2803d1f44d696a83fca670f530234",
          "message": "Merge pull request #40 from hseeberger/ci/bench-clean-criterion\n\nci: clean stale criterion data before benchmarking",
          "timestamp": "2026-08-25T12:50:40+02:00",
          "tree_id": "92a7665636244a9fda6db6185615cd437ae14c08",
          "url": "https://github.com/hseeberger/tellus/commit/e816039a5bd2803d1f44d696a83fca670f530234"
        },
        "date": 1787655123170,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 13051133,
            "range": "± 380005",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12850419,
            "range": "± 222707",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 750548,
            "range": "± 19158",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 823733,
            "range": "± 6682",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4934255,
            "range": "± 206153",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3438672,
            "range": "± 9176",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "9d30145b85871fb7f7bf0fad071eababa51c5634",
          "message": "Merge pull request #43 from hseeberger/dependabot/github_actions/ci-minor-492e2c7bf6\n\nci(deps): bump taiki-e/install-action from 2.85.11 to 2.86.3 in the ci-minor group",
          "timestamp": "2026-08-26T08:13:32+02:00",
          "tree_id": "7594b66bfd7abf97a6fe7fcb7214547c43badda4",
          "url": "https://github.com/hseeberger/tellus/commit/9d30145b85871fb7f7bf0fad071eababa51c5634"
        },
        "date": 1787724927319,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12856282,
            "range": "± 325185",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 14845775,
            "range": "± 60858",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 644967,
            "range": "± 9962",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 804735,
            "range": "± 31842",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4946714,
            "range": "± 163515",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3572652,
            "range": "± 64889",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}