window.BENCHMARK_DATA = {
  "lastUpdate": 1787908004269,
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
          "id": "105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829",
          "message": "Merge pull request #54 from hseeberger/release/tellus-0.2.0\n\nchore: release tellus 0.2.0 and tellus-persistence-postgres 0.1.0",
          "timestamp": "2026-08-27T23:33:37+02:00",
          "tree_id": "173bf718a78bdb32ab258f04cd95f1f2a59eefda",
          "url": "https://github.com/hseeberger/tellus/commit/105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829"
        },
        "date": 1787866532863,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 9221059,
            "range": "± 174124",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 8928111,
            "range": "± 429757",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 548536,
            "range": "± 2599",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 604777,
            "range": "± 9378",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4208362,
            "range": "± 97396",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 2758914,
            "range": "± 13823",
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
          "id": "f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf",
          "message": "Merge pull request #55 from hseeberger/chore/bench-variance\n\nchore: pin bench runtime threads and raise sampling",
          "timestamp": "2026-08-28T00:03:23+02:00",
          "tree_id": "e70ac7f0b854adff8d7a19ee4fe80d46869ed424",
          "url": "https://github.com/hseeberger/tellus/commit/f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf"
        },
        "date": 1787868414923,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 11609576,
            "range": "± 120307",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 10615836,
            "range": "± 62633",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 663926,
            "range": "± 9446",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 792834,
            "range": "± 12230",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4928923,
            "range": "± 110032",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3447873,
            "range": "± 28993",
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
          "id": "9392dec4a4ce622e105058bcd1c0f7f1804c001e",
          "message": "Merge pull request #56 from hseeberger/ci/no-tag-trigger\n\nci: drop tag trigger from CI workflow",
          "timestamp": "2026-08-28T00:13:21+02:00",
          "tree_id": "e99834098d29fbda0b78763d4c6c5488c76f4cc0",
          "url": "https://github.com/hseeberger/tellus/commit/9392dec4a4ce622e105058bcd1c0f7f1804c001e"
        },
        "date": 1787869010486,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12292762,
            "range": "± 243687",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 11739486,
            "range": "± 146448",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 713348,
            "range": "± 8979",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 801746,
            "range": "± 4419",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 5685341,
            "range": "± 172098",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3576901,
            "range": "± 21364",
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
          "id": "459ca7503272e953ef66a61aa0f47d6732a6b42c",
          "message": "Merge pull request #59 from hseeberger/ci/informational-bench-gate\n\nci: make bench comparison informational",
          "timestamp": "2026-08-28T11:03:13+02:00",
          "tree_id": "dcc0235bc8c9ae9325d2f84a4d24a06a0b774da1",
          "url": "https://github.com/hseeberger/tellus/commit/459ca7503272e953ef66a61aa0f47d6732a6b42c"
        },
        "date": 1787908001228,
        "tool": "cargo",
        "benches": [
          {
            "name": "flood/unbounded",
            "value": 12188311,
            "range": "± 237619",
            "unit": "ns/iter"
          },
          {
            "name": "flood/bounded",
            "value": 12485920,
            "range": "± 243509",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/1",
            "value": 676289,
            "range": "± 18882",
            "unit": "ns/iter"
          },
          {
            "name": "ping_pong/pairs/4",
            "value": 795321,
            "range": "± 4502",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/4",
            "value": 4773845,
            "range": "± 91754",
            "unit": "ns/iter"
          },
          {
            "name": "fan_out/workers/16",
            "value": 3398305,
            "range": "± 19597",
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
          "id": "105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829",
          "message": "Merge pull request #54 from hseeberger/release/tellus-0.2.0\n\nchore: release tellus 0.2.0 and tellus-persistence-postgres 0.1.0",
          "timestamp": "2026-08-27T23:33:37+02:00",
          "tree_id": "173bf718a78bdb32ab258f04cd95f1f2a59eefda",
          "url": "https://github.com/hseeberger/tellus/commit/105234ca3f6a7f3c36f7bb95f8bb8b1fef19e829"
        },
        "date": 1787866535284,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 2457849,
            "range": "± 20934",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2413849,
            "range": "± 6395",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1226885,
            "range": "± 9473",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 149603,
            "range": "± 5594",
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
          "id": "f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf",
          "message": "Merge pull request #55 from hseeberger/chore/bench-variance\n\nchore: pin bench runtime threads and raise sampling",
          "timestamp": "2026-08-28T00:03:23+02:00",
          "tree_id": "e70ac7f0b854adff8d7a19ee4fe80d46869ed424",
          "url": "https://github.com/hseeberger/tellus/commit/f7e8a3ec45a0f24d8c8bbc25a3a8f12714245fcf"
        },
        "date": 1787868417430,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3039433,
            "range": "± 19013",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 2934554,
            "range": "± 12096",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1629315,
            "range": "± 8422",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 209484,
            "range": "± 6380",
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
          "id": "9392dec4a4ce622e105058bcd1c0f7f1804c001e",
          "message": "Merge pull request #56 from hseeberger/ci/no-tag-trigger\n\nci: drop tag trigger from CI workflow",
          "timestamp": "2026-08-28T00:13:21+02:00",
          "tree_id": "e99834098d29fbda0b78763d4c6c5488c76f4cc0",
          "url": "https://github.com/hseeberger/tellus/commit/9392dec4a4ce622e105058bcd1c0f7f1804c001e"
        },
        "date": 1787869013244,
        "tool": "cargo",
        "benches": [
          {
            "name": "persist/no_snapshots",
            "value": 3142533,
            "range": "± 20834",
            "unit": "ns/iter"
          },
          {
            "name": "persist/snapshots",
            "value": 3112321,
            "range": "± 28054",
            "unit": "ns/iter"
          },
          {
            "name": "recover/replay",
            "value": 1484287,
            "range": "± 3327",
            "unit": "ns/iter"
          },
          {
            "name": "recover/snapshot",
            "value": 190920,
            "range": "± 6711",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}