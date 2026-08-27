window.BENCHMARK_DATA = {
  "lastUpdate": 1787846117395,
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
          "id": "122046c900e5309a4567d0a13b3532cb9a4ab17a",
          "message": "Merge pull request #49 from hseeberger/build/publish-persistence-postgres\n\nbuild: publish tellus-persistence-postgres to crates.io",
          "timestamp": "2026-08-26T11:29:52+02:00",
          "tree_id": "29a63385a27e372b211c583741b956b1e1ccbec7",
          "url": "https://github.com/hseeberger/tellus/commit/122046c900e5309a4567d0a13b3532cb9a4ab17a"
        },
        "date": 1787736711858,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11858411,
            "range": "± 151430",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11826622,
            "range": "± 142760",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 686555,
            "range": "± 10319",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 771690,
            "range": "± 4933",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5601296,
            "range": "± 110149",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3555818,
            "range": "± 9906",
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
          "id": "0f679455839c1ea6d716352859a2dcd95ed40711",
          "message": "Merge pull request #51 from hseeberger/docs/cargo-add-install\n\ndocs: install via cargo add instead of pinned versions",
          "timestamp": "2026-08-27T17:42:04+02:00",
          "tree_id": "95736cb2e525bf5f787649f3157aa160bfe0b767",
          "url": "https://github.com/hseeberger/tellus/commit/0f679455839c1ea6d716352859a2dcd95ed40711"
        },
        "date": 1787845457377,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 26640174,
            "range": "± 1317423",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 33932148,
            "range": "± 1002903",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 915378,
            "range": "± 6953",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 1118685,
            "range": "± 37016",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 9509958,
            "range": "± 221441",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 8874525,
            "range": "± 228582",
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
          "id": "779ef2f79d25a8532753cbbb110a8b93ce8bcbb9",
          "message": "Merge pull request #52 from hseeberger/build/independent-crate-versions\n\nbuild: use independent versions per crate",
          "timestamp": "2026-08-27T17:53:17+02:00",
          "tree_id": "bb4b2115dc6995dd7741bc79ba515d70bc94d278",
          "url": "https://github.com/hseeberger/tellus/commit/779ef2f79d25a8532753cbbb110a8b93ce8bcbb9"
        },
        "date": 1787846112870,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9100745,
            "range": "± 111013",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11188195,
            "range": "± 103266",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 562944,
            "range": "± 4973",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 613272,
            "range": "± 3294",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4339785,
            "range": "± 30941",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2779179,
            "range": "± 10062",
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
        "date": 1787735652483,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3010148,
            "range": "± 10571",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2941202,
            "range": "± 5354",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1632694,
            "range": "± 7700",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 210074,
            "range": "± 3640",
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
          "id": "122046c900e5309a4567d0a13b3532cb9a4ab17a",
          "message": "Merge pull request #49 from hseeberger/build/publish-persistence-postgres\n\nbuild: publish tellus-persistence-postgres to crates.io",
          "timestamp": "2026-08-26T11:29:52+02:00",
          "tree_id": "29a63385a27e372b211c583741b956b1e1ccbec7",
          "url": "https://github.com/hseeberger/tellus/commit/122046c900e5309a4567d0a13b3532cb9a4ab17a"
        },
        "date": 1787736713568,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3202949,
            "range": "± 12756",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3271057,
            "range": "± 30443",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1535906,
            "range": "± 2947",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 184317,
            "range": "± 1025",
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
          "id": "0f679455839c1ea6d716352859a2dcd95ed40711",
          "message": "Merge pull request #51 from hseeberger/docs/cargo-add-install\n\ndocs: install via cargo add instead of pinned versions",
          "timestamp": "2026-08-27T17:42:04+02:00",
          "tree_id": "95736cb2e525bf5f787649f3157aa160bfe0b767",
          "url": "https://github.com/hseeberger/tellus/commit/0f679455839c1ea6d716352859a2dcd95ed40711"
        },
        "date": 1787845460631,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3404791,
            "range": "± 52937",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3385909,
            "range": "± 47930",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1311595,
            "range": "± 4937",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 167741,
            "range": "± 3752",
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
          "id": "779ef2f79d25a8532753cbbb110a8b93ce8bcbb9",
          "message": "Merge pull request #52 from hseeberger/build/independent-crate-versions\n\nbuild: use independent versions per crate",
          "timestamp": "2026-08-27T17:53:17+02:00",
          "tree_id": "bb4b2115dc6995dd7741bc79ba515d70bc94d278",
          "url": "https://github.com/hseeberger/tellus/commit/779ef2f79d25a8532753cbbb110a8b93ce8bcbb9"
        },
        "date": 1787846116378,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2517560,
            "range": "± 26564",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2639646,
            "range": "± 20860",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1195492,
            "range": "± 6157",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 141635,
            "range": "± 3048",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}