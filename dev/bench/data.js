window.BENCHMARK_DATA = {
  "lastUpdate": 1787735651261,
  "repoUrl": "https://github.com/hseeberger/tellus",
  "entries": {
    "Core": [
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
          "id": "80e7177421e90a55b20e7cf1cbbc914903cc80ec",
          "message": "Merge pull request #21 from hseeberger/feat/persistence\n\nfeat: add event-sourced persistence",
          "timestamp": "2026-08-26T08:53:42+02:00",
          "tree_id": "98716b2cae996f3fa4c9ad53d94298f8920d71f0",
          "url": "https://github.com/hseeberger/tellus/commit/80e7177421e90a55b20e7cf1cbbc914903cc80ec"
        },
        "date": 1787727379906,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11282563,
            "range": "± 57976",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13138624,
            "range": "± 82600",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 697160,
            "range": "± 9731",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 799337,
            "range": "± 3521",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5123831,
            "range": "± 133739",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3498352,
            "range": "± 32679",
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
          "id": "e51adc4128f5c749e297ca3b3cca92e28d707702",
          "message": "Merge pull request #47 from hseeberger/docs/refresh-after-persistence\n\ndocs: refresh docs after persistence landed",
          "timestamp": "2026-08-26T11:11:33+02:00",
          "tree_id": "25dc57378d1d9b392e4be70e040341db6a59bfdb",
          "url": "https://github.com/hseeberger/tellus/commit/e51adc4128f5c749e297ca3b3cca92e28d707702"
        },
        "date": 1787735650736,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12160345,
            "range": "± 412558",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 13390135,
            "range": "± 112011",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 669611,
            "range": "± 40201",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 765980,
            "range": "± 48621",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4879367,
            "range": "± 68115",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3428354,
            "range": "± 37869",
            "unit": "ns/iter"
          }
        ]
      }
    ],
    "Persistence": [
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
          "id": "80e7177421e90a55b20e7cf1cbbc914903cc80ec",
          "message": "Merge pull request #21 from hseeberger/feat/persistence\n\nfeat: add event-sourced persistence",
          "timestamp": "2026-08-26T08:53:42+02:00",
          "tree_id": "98716b2cae996f3fa4c9ad53d94298f8920d71f0",
          "url": "https://github.com/hseeberger/tellus/commit/80e7177421e90a55b20e7cf1cbbc914903cc80ec"
        },
        "date": 1787727382366,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2963895,
            "range": "± 11288",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2916345,
            "range": "± 15207",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1755749,
            "range": "± 13886",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 208540,
            "range": "± 6495",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}