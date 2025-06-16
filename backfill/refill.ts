// /* eslint-disable @typescript-eslint/consistent-type-imports, no-await-in-loop */
// import * as sdk from "@defillama/sdk";
// import { getBlocks } from "@defillama/sdk/build/util/blocks";
// import * as AWS from "aws-sdk";
// import chalk from "chalk";
// import { diff, Diff } from "deep-diff";
// import * as readline from "readline";
// import { importAdapter } from "../src/peggedAssets/utils/importAdapter";
// import peggedData from "../src/peggedData/peggedData";

// /*──────────────────── CLI / AWS ───────────────────*/
// const argv = process.argv.slice(2);
// const arg = (k: string) =>
//   argv.find((a) => a.startsWith(`--${k}=`))?.split("=")[1];

// const adapterName = arg("adapter");
// const start = Number(arg("start"));
// const end = Number(arg("end"));
// const tableName = arg("table");
// const region = arg("region") ?? "eu-west-1";
// const profile = arg("profile");
// const apply = argv.includes("--apply");
// let autoYes = argv.includes("--yes");
// const sleepMs = Number(arg("sleep") ?? 0);
// const ignorePaths = (arg("ignore") ?? "")
//   .split(",")
//   .map((s) => s.trim())
//   .filter(Boolean);

// if (!adapterName || Number.isNaN(start) || Number.isNaN(end))
//   throw new Error("--adapter --start --end requis");
// if (!tableName) throw new Error("--table manquant");

// process.env.AWS_SDK_JS_SUPPRESS_MAINTENANCE_MODE_MESSAGE = "true";
// if (profile)
//   AWS.config.credentials = new AWS.SharedIniFileCredentials({ profile });
// AWS.config.update({ region });
// const db = new AWS.DynamoDB.DocumentClient();

// /*────────── TYPES ─────────*/
// type Pegged = Record<string, number>;
// type ProvMap = Record<string, Record<string, number>>;
// interface Snap {
//   minted: number;
//   unreleased: number;
//   bridgesOut: ProvMap;
// }
// interface Chain {
//   minted: Pegged;
//   unreleased: Pegged;
//   circulating: Pegged;
//   bridgedTo: {
//     peggedUSD: number;
//     bridges: Record<string, Record<string, { amount: number }>>;
//   };
//   [dst: string]: any;
// }
// type DayRec = Record<string, any> & { PK: string; SK: number };

// /*────────── CONSTS ───────*/
// const day = 86_400;
// const backfill: Record<string, number> = {
//   ethereum: 1438214400,
//   bsc: 1598591408,
//   arbitrum: 1640995200,
//   optimism: 1640995200,
//   fantom: 1577404800,
//   polygon: 1585658400,
//   era: 1679670000,
//   base: 1689260400,
//   linea: 1689070800,
//   mantle: 1689595200,
//   scroll: 1697541600,
//   manta: 1694527200,
//   blast: 1708819200,
//   avax: 1714521600,
//   berachain: 1737342000
// };
// const THRESHOLD = 0.01; // 1 %

// /*────────── UTIL ───────*/
// const rl = readline.createInterface({
//   input: process.stdin,
//   output: process.stdout,
// });
// const ask = (q: string) => new Promise<string>((r) => rl.question(q, r));
// const askYN = (q: string) => (autoYes ? Promise.resolve("y") : ask(q));
// const askYNmanual = (q: string) => ask(q);
// const wait = (ms: number) =>
//   ms ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve();
// const can = (s: string) => s.toLowerCase();
// const tsRange = (s: number, e: number) => {
//   const a: number[] = [];
//   for (let t = s; t <= e; t += day) a.push(t);
//   return a;
// };

// /*────────────────── helpers ──────────────────*/
// const ALIAS: Record<string, string> = { "not-found": "ethereum" };
// const canonical = (s: string) => ALIAS[can(s)] ?? can(s);

// /*────────── IGNORE helper ───────*/
// function deepGet(o: any, p: string[]): any {
//   return p.reduce((x, k) => (x && typeof x === "object" ? x[k] : undefined), o);
// }
// function deepSet(o: any, p: string[], v: any) {
//   let cur = o;
//   for (let i = 0; i < p.length - 1; i++) {
//     const k = p[i];
//     if (!cur[k] || typeof cur[k] !== "object") cur[k] = {};
//     cur = cur[k];
//   }
//   cur[p[p.length - 1]] = v;
// }
// function applyIgnorePaths(
//   tgt: Record<string, any>,
//   src: Record<string, any> | undefined,
//   paths: string[]
// ) {
//   if (!src) return;
//   for (const p of paths) {
//     const seg = p.split(".");
//     const prev = deepGet(src, seg);
//     if (prev !== undefined) deepSet(tgt, seg, prev);
//   }
// }

// /*────────── BLOCK cache ───────*/
// const blkCache = new Map<string, Promise<Record<string, number | undefined>>>();
// function blocks(ts: number, chs: string[]) {
//   const k = `${ts}/${chs.join(",")}`;
//   if (!blkCache.has(k))
//     blkCache.set(
//       k,
//       (async () => {
//         const r = await Promise.all(
//           chs.map(async (c) => {
//             try {
//               const b = await getBlocks(ts, [c]);
//               return { c, b: b.chainBlocks?.[c] };
//             } catch {
//               return { c, b: undefined };
//             }
//           })
//         );
//         return r.reduce(
//           (m, { c, b }) => ({ ...m, [c]: b }),
//           {} as Record<string, number | undefined>
//         );
//       })()
//     );
//   return blkCache.get(k)!;
// }

// /*────────── NORMALISE snap ───────*/
// function normalise(raw: Snap, pt: string): Chain {
//   const out: Chain = {
//     minted: { [pt]: raw.minted },
//     unreleased: { [pt]: raw.unreleased },
//     circulating: { [pt]: 0 },
//     bridgedTo: { peggedUSD: 0, bridges: {} },
//   };
//   for (const [prov, dests] of Object.entries(raw.bridgesOut)) {
//     const pMap: Record<string, { amount: number }> = {};
//     for (const [dstKey, amt] of Object.entries(dests)) {
//       const amount = Number(amt);
//       if (amount <= 0 || Number.isNaN(amount)) continue;
//       pMap[can(dstKey)] = { amount };
//       out.bridgedTo.peggedUSD += amount;
//     }
//     if (Object.keys(pMap).length) out.bridgedTo.bridges[prov] = pMap;
//   }
//   return out;
// }

// /*────────── mergeChain ───────*/
// function mergeChain(prev: Chain, snap: Chain, pt: string): Chain {
//   const res: Chain = structuredClone(prev);

//   if (snap.minted[pt]     !== 0) res.minted[pt]     = snap.minted[pt];
//   if (snap.unreleased[pt] !== 0) res.unreleased[pt] = snap.unreleased[pt];

//   for (const [prov, dests] of Object.entries(snap.bridgedTo.bridges)) {
//     res.bridgedTo.bridges[prov] = structuredClone(dests);
//   }

//   return res;
// }
// // function mergeChain(prev: Chain, snap: Chain, pt: string): Chain {
// //   const res: Chain = structuredClone(prev);
// //   if (snap.minted[pt] !== 0) res.minted[pt] = snap.minted[pt];
// //   if (snap.unreleased[pt] !== 0) res.unreleased[pt] = snap.unreleased[pt];
// //   for (const [prov, dests] of Object.entries(snap.bridgedTo.bridges)) {
// //     res.bridgedTo.bridges[prov] ??= {};
// //     for (const [dst, { amount }] of Object.entries(dests))
// //       res.bridgedTo.bridges[prov][dst] = { amount };
// //   }
// //   return res;
// // }
// function isAllZero(c: Chain, pt: string) {
//   return (
//     c.minted[pt] === 0 &&
//     c.unreleased[pt] === 0 &&
//     c.circulating[pt] === 0 &&
//     c.bridgedTo.peggedUSD === 0 &&
//     !Object.keys(c.bridgedTo.bridges).length
//   );
// }

// /*────────── STANDARDISE ───────*/
// function standardise(rec: DayRec, pt: string) {
//   const meta = ["PK", "SK", "totalCirculating"];
//   const chains = Object.keys(rec).filter((k) => !meta.includes(k));

//   const inbound: Record<string, number> = {},
//     outbound: Record<string, number> = {},
//     mirror: Record<string, Record<string, number>> = {};
//   chains.forEach((c) => {
//     inbound[c] = 0;
//     outbound[c] = 0;
//     mirror[c] = {};
//   });

//   for (const dst of chains) {
//     let tot = 0;
//     for (const prov of Object.values(rec[dst].bridgedTo.bridges) as any[]) {
//       for (const [srcRaw, { amount }] of Object.entries(prov) as any) {
//         const src = canonical(srcRaw);
//         inbound[dst] += amount;
//         outbound[src] += amount;
//         tot += amount;
//         mirror[dst][src] = (mirror[dst][src] ?? 0) + amount;
//       }
//     }
//     rec[dst].bridgedTo.peggedUSD = tot;
//   }

//   for (const ch of chains) {
//     const n = rec[ch] as Chain;

//     for (const k of Object.keys(n))
//       if (!["minted", "unreleased", "circulating", "bridgedTo"].includes(k))
//         delete (n as any)[k];

//     for (const [srcRaw, amt] of Object.entries(mirror[ch]))
//       n[canonical(srcRaw)] = { [pt]: amt };

//     const unreleasedUsed =
//       ch === "tron"
//         ? Math.max(0, n.unreleased[pt] - outbound[ch])
//         : n.unreleased[pt];

//     n.circulating[pt] =
//       n.minted[pt] - unreleasedUsed + inbound[ch] - outbound[ch];
//   }


//   // for (const ch of chains) {
//   //   const n = rec[ch] as Chain;
//   //   for (const k of Object.keys(n))
//   //     if (!["minted", "unreleased", "circulating", "bridgedTo"].includes(k))
//   //       delete (n as any)[k];

//   //   for (const [srcRaw, amt] of Object.entries(mirror[ch])) n[canonical(srcRaw)] = { [pt]: amt };

//   //   const locks = Object.values(mirror).reduce((s, m) => s + (m[ch] ?? 0), 0);
//   //   if (n.unreleased[pt] < locks - 1e-6) n.unreleased[pt] = locks;

//   //   const unreleasedUsed =
//   //     ch === "tron"
//   //       ? Math.max(0, n.unreleased[pt] - outbound[ch])
//   //       : n.unreleased[pt];

//   //   n.circulating[pt] =
//   //     n.minted[pt] /*-unreleasedUsed*/ + inbound[ch] - outbound[ch];
//   // }

//   const total = chains.reduce((s, c) => s + rec[c].circulating[pt], 0);
//   rec.totalCirculating = {
//     circulating: { [pt]: total },
//     unreleased: { [pt]: 0 },
//   };
// }

// /*──── UPGRADE legacy ───*/
// function upgrade(rec: DayRec, pt: string): DayRec {
//   const meta = ["PK", "SK", "totalCirculating"];
//   const chains = Object.keys(rec).filter((k) => !meta.includes(k));
//   for (const ch of chains) {
//     const n = rec[ch] as Chain;
//     n.minted = { [pt]: Number((n.minted as any)?.[pt] ?? n.minted ?? 0) };
//     n.unreleased = {
//       [pt]: Number((n.unreleased as any)?.[pt] ?? n.unreleased ?? 0),
//     };
//     delete (n as any).minted?.bridges;
//     delete (n as any).unreleased?.bridges;

//     const clean: Record<string, Record<string, { amount: number }>> = {};
//     n.bridgedTo ??= { peggedUSD: 0, bridges: {} };

//     for (const [prov, dests] of Object.entries(n.bridgedTo.bridges ?? {})) {
//       const pMap: Record<string, { amount: number }> = {};
//       for (const [k, v] of Object.entries(dests as Record<string, any>)) {
//         if (k === "source") continue;
//         const o = typeof v === "number" ? { amount: v } : v ?? {};
//         const amt = Number(o.amount ?? o.peggedUSD ?? o[pt] ?? 0);
//         if (amt <= 0 || Number.isNaN(amt)) continue;
//         let dst = k === "amount" ? canonical(String(o.source ?? "ethereum")) : canonical(k);
//         pMap[dst] = { amount: amt };
//       }
//       if (Object.keys(pMap).length) clean[prov] = pMap;
//     }
//     n.bridgedTo.bridges = clean;
//     n.bridgedTo.peggedUSD = 0;
//   }
//   standardise(rec, pt);
//   return rec;
// }

// /*────────────────── MAIN ─────────────────*/
// (async () => {
//   console.log(
//     chalk.bold(`Starting run${apply ? " with --apply" : " (dry-run)"} …`)
//   );
//   if (ignorePaths.length)
//     console.log(chalk.blue(`Ignoring paths: ${ignorePaths.join(", ")}`));

//   const meta = (peggedData as any[]).find((m) => m.gecko_id === adapterName)!;
//   const pt = meta.pegType || "peggedUSD";
//   const adapter = importAdapter(meta);
//   const stamps = tsRange(start, end);
//   const adapterId = meta.id;

//   const avail = Object.keys(adapter).map(can);
//   const back = avail.filter((c) => backfill[c]! <= start);
//   console.log(chalk.bold("\n⎯⎯⎯ Available ⎯⎯⎯"));
//   console.log(chalk.cyan(avail.join(", ")));
//   console.log(chalk.bold("\n⎯⎯⎯ Backfillable ⎯⎯⎯"));
//   console.log(chalk.cyan(back.join(", ")));
//   if (!back.length) {
//     console.log(chalk.red("No backfillable chain"));
//     rl.close();
//     process.exit(0);
//   }

//   let selected: string[] = [];
//   let dyn = false;
//   while (true) {
//     const a = await ask(chalk.yellow("\nChains? ('all' or list) "));
//     if (a.toLowerCase() === "all") {
//       dyn = true;
//       selected = back;
//       break;
//     }
//     const list = a
//       .split(",")
//       .map((s) => can(s.trim()))
//       .filter(Boolean);
//     const bad = list.filter((c) => !back.includes(c));
//     if (bad.length) {
//       console.log(chalk.red(`Invalid: ${bad.join(", ")}`));
//       continue;
//     }
//     selected = list;
//     break;
//   }

//   for (const ts of stamps) {
//     console.log(
//       chalk.magenta(
//         `\n—— ${ts} (${new Date(ts * 1e3).toISOString().slice(0, 10)}) ——`
//       )
//     );

//     const chainsRaw = dyn
//       ? Object.keys(adapter).filter((c) => backfill[can(c)] <= ts)
//       : selected;
//     const key = { PK: `dailyPeggedBalances#${adapterId}`, SK: ts };

//     const prevRaw = (await db.get({ TableName: tableName, Key: key }).promise())
//       .Item as DayRec | undefined;
//     const prev = prevRaw ? upgrade(prevRaw, pt) : undefined;

//     const snaps: Record<string, Chain> = {},
//       errs: string[] = [];
//     await Promise.all(
//       chainsRaw.map(async (raw) => {
//         try {
//           const blk = await blocks(ts, [raw]);
//           const api = new sdk.ChainApi({
//             chain: raw,
//             timestamp: ts,
//             block: blk[raw],
//           });
//           const snap: Snap = { minted: 0, unreleased: 0, bridgesOut: {} };

//           for (const meth of ["minted", "unreleased"] as const) {
//             const fn = adapter[raw]?.[meth];
//             if (!fn) continue;
//             try {
//               const f = typeof (fn as any).then === "function" ? await fn : fn;
//               const val = (await f(api)) as number | Pegged;
//               snap[meth] =
//                 typeof val === "number" ? val : (val as Pegged)[pt] ?? 0;
//             } catch (e: any) {
//               errs.push(`${raw}.${meth}: ${e.message || e}`);
//             }
//           }

//           for (const [meth, fnOr] of Object.entries(adapter[raw] ?? {})) {
//             if (["minted", "unreleased"].includes(meth)) continue;
//             const fn =
//               typeof (fnOr as any).then === "function" ? await fnOr : fnOr;
//             try {
//               const out = typeof fn === "function" ? await fn(api) : undefined;
//               if (!out || typeof out !== "object" || !("bridges" in out))
//                 continue;
//               const bridges = out.bridges as Record<
//                 string,
//                 Record<string, { amount: number }>
//               >;
//               for (const [prov, dests] of Object.entries(bridges)) {
//                 snap.bridgesOut[prov] ??= {};
//                 for (const [dst, { amount }] of Object.entries(dests))
//                   snap.bridgesOut[prov][can(dst)] = amount;
//               }
//             } catch (e: any) {
//               errs.push(`${raw}.${meth}: ${e.message || e}`);
//             }
//           }
//           snaps[can(raw)] = normalise(snap, pt);
//           if (isAllZero(snaps[can(raw)], pt)) delete snaps[can(raw)];
//         } catch (e: any) {
//           errs.push(`${raw}: ${e.message || e}`);
//         }
//       })
//     );

//     if (errs.length) {
//       console.log(chalk.red(`❌ ${errs.length} errors:`));
//       errs.forEach((e) => console.log("  -", e));
//       if (
//         (
//           await askYN(chalk.yellow("Continue despite errors ? (y/N) "))
//         ).toLowerCase() !== "y"
//       ) {
//         await wait(sleepMs);
//         continue;
//       }
//     }
//     if (!Object.keys(snaps).length) {
//       console.log(chalk.blue("Nothing to store"));
//       await wait(sleepMs);
//       continue;
//     }

//     const rec: DayRec = prev
//       ? structuredClone(prev)
//       : { PK: key.PK, SK: key.SK };
//     for (const [c, s] of Object.entries(snaps))
//       rec[c] = prev && prev[c] ? mergeChain(prev[c], s, pt) : s;

//     standardise(rec, pt);

//     /*──────── ignorePaths ───────*/
//     applyIgnorePaths(rec, prev, ignorePaths);

//     /*──────── Δ par chaîne ───────*/
//     console.log(chalk.bold("\nΔ per chain (circulating)"));
//     Object.keys(rec)
//       .filter((k) => !["PK", "SK", "totalCirculating"].includes(k))
//       .forEach((c) => {
//         const o = prev?.[c]?.circulating?.[pt] || 0;
//         const n = rec[c].circulating[pt];
//         const d = n - o;
//         if (d !== 0)
//           console.log(
//             `${c.padEnd(12)} : ${o.toLocaleString()} → ${n.toLocaleString()} ` +
//               chalk[d > 0 ? "green" : "red"](
//                 `(${d > 0 ? "+" : ""}${d.toLocaleString()})`
//               )
//           );
//       });

//     /*──────── Preview + DIFF ───────*/
//     if (
//       (
//         await askYN(chalk.yellow("Show full merged object? (y/N) "))
//       ).toLowerCase() === "y"
//     ) {
//       console.log(JSON.stringify(rec,null,2));
//       if (prev) {
//         const changes = diff(prev, rec) as Diff<any, any>[] | undefined;
//         if (changes) {
//           console.log(chalk.bold("\n⎯⎯⎯ DIFF ⎯⎯⎯"));
//           for (const d of changes) {
//             const loc = d.path?.join(".") ?? "",
//               lhs = JSON.stringify((d as any).lhs),
//               rhs = JSON.stringify((d as any).rhs);
//             if (d.kind === "E")
//               console.log(
//                 `${chalk.cyan(loc)}: ${chalk.red(lhs)} → ${chalk.green(rhs)}`
//               );
//             if (d.kind === "N")
//               console.log(`${chalk.cyan(loc)} added:\n${chalk.green(rhs)}`);
//             if (d.kind === "D")
//               console.log(`${chalk.cyan(loc)} removed:\n${chalk.red(lhs)}`);
//           }
//         }
//       }
//     }

//     /*──────── Warden totalCirculating ───────*/
//     if (prev) {
//       const prevTot = prev.totalCirculating.circulating[pt];
//       let newTot = rec.totalCirculating.circulating[pt];
//       const diffAbs = Math.abs(newTot - prevTot);
//       const diffPct = prevTot ? diffAbs / prevTot : 0;

//       if (newTot < prevTot) {
//         rec.totalCirculating.circulating[pt] = prevTot;
//       } else if (newTot > prevTot && diffPct > THRESHOLD) {
//         console.log(chalk.bold("\nΔ totalCirculating"));
//         console.log(
//           chalk.red(`${prevTot.toLocaleString()} → ${newTot.toLocaleString()}`)
//         );
//         const ok = await askYNmanual(
//           chalk.yellow(
//             "⚠  Recalculated totalCirculating is HIGHER. Accept? (y/N) "
//           )
//         );
//         if (ok.trim().toLowerCase() !== "y") {
//           await wait(sleepMs);
//           continue;
//         }
//       }
//     }

//     /*──────── write ───────*/
//     if (apply) {
//       if (
//         (
//           await askYN(chalk.yellow("Apply to DynamoDB? (y/N) "))
//         ).toLowerCase() === "y"
//       ) {
//         try {
//           await db.put({ TableName: tableName, Item: rec }).promise();
//           console.log(chalk.green("✔ applied"));
//         } catch (e) {
//           console.error(chalk.red("❌ write failed"), e);
//           if (
//             (
//               await askYN(chalk.yellow("Continue next? (y/N) "))
//             ).toLowerCase() !== "y"
//           ) {
//             rl.close();
//             process.exit(1);
//           }
//         }
//       } else console.log(chalk.blue("Push skipped"));
//     }

//     if (
//       (
//         await askYN(chalk.yellow("Continue to next timestamp? (y/N) "))
//       ).toLowerCase() !== "y"
//     ) {
//       console.log(chalk.blue("Process interrupted"));
//       rl.close();
//       process.exit(0);
//     }
//     await wait(sleepMs);
//   }

//   console.log(chalk.bold("\nDone."));
//   rl.close();
//   process.exit(0);
// })().catch((e) => {
//   console.error(chalk.red("Fatal error"), e);
//   process.exit(1);
// });

/* eslint-disable @typescript-eslint/consistent-type-imports,no-await-in-loop */

import * as sdk from "@defillama/sdk";
import { getBlocks } from "@defillama/sdk/build/util/blocks";
import * as AWS from "aws-sdk";
import chalk from "chalk";
import { diff, Diff } from "deep-diff";
import * as readline from "readline";
import { importAdapter } from "../src/peggedAssets/utils/importAdapter";
import peggedData from "../src/peggedData/peggedData";

/*──────────────────── CLI / AWS ───────────────────*/
const argv = process.argv.slice(2);
const arg = (k: string) => argv.find((a) => a.startsWith(`--${k}=`))?.split("=")[1];

const adapterName = arg("adapter");
const start       = Number(arg("start"));
const end         = Number(arg("end"));
const tableName   = arg("table");
const region      = arg("region")  ?? "eu-west-1";
const profile     = arg("profile");
const apply       = argv.includes("--apply");
let   autoYes     = argv.includes("--yes");
const sleepMs     = Number(arg("sleep") ?? 0);
const ignorePaths = (arg("ignore") ?? "")
  .split(",").map((s) => s.trim()).filter(Boolean);

if (!adapterName || Number.isNaN(start) || Number.isNaN(end))
  throw new Error("--adapter --start --end requis");
if (!tableName) throw new Error("--table manquant");

process.env.AWS_SDK_JS_SUPPRESS_MAINTENANCE_MODE_MESSAGE = "true";
if (profile) AWS.config.credentials = new AWS.SharedIniFileCredentials({ profile });
AWS.config.update({ region });
const db = new AWS.DynamoDB.DocumentClient();

/*────────── TYPES ─────────*/
type Pegged  = Record<string, number>;
type ProvMap = Record<string, Record<string, number>>;
interface Snap {
  minted: number;
  unreleased: number;
  bridgesOut: ProvMap;
}
interface Chain {
  minted: Pegged;
  unreleased: Pegged;
  circulating: Pegged;
  bridgedTo: {
    peggedUSD: number;
    bridges: Record<string, Record<string, { amount: number }>>;
  };
  [dst: string]: any;
}
type DayRec = Record<string, any> & { PK: string; SK: number };

/*────────── CONSTS ───────*/
const day = 86_400;
const backfill: Record<string, number> = {
  ethereum: 1438214400,
  bsc: 1598591408,
  arbitrum: 1640995200,
  optimism: 1640995200,
  fantom: 1577404800,
  polygon: 1585658400,
  era: 1679670000,
  base: 1689260400,
  linea: 1689070800,
  mantle: 1689595200,
  scroll: 1697541600,
  manta: 1694527200,
  blast: 1708819200,
  avax: 1714521600,
  berachain: 1737342000,
  // sonic: 1733011200
};
const THRESHOLD = 0.01; // 1 %

/*────────── UTIL ───────*/
const rl  = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q: string) => new Promise<string>((r) => rl.question(q, r));
const askYN       = (q: string) => (autoYes ? Promise.resolve("y") : ask(q));
const askYNmanual = (q: string) => ask(q);
const wait  = (ms: number) => (ms ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve());
const lower = (s: string) => s.toLowerCase();
const tsRange = (s: number, e: number) => { const a: number[] = []; for (let t = s; t <= e; t += day) a.push(t); return a; };

/*────────────────── helpers ──────────────────*/
/* providers (bridge) aliases */
const ALIAS: Record<string, string> = { "not-found": "ethereum" };
const canonical = (s: string) => ALIAS[lower(s)] ?? lower(s);

/* chain aliases (storage key)            avalanche -> avax */
const CHAIN_ALIAS: Record<string, string> = { avalanche: "avax" };
const canonicalChain = (s: string) => CHAIN_ALIAS[lower(s)] ?? lower(s);

/*────────── IGNORE helper ───────*/
function deepGet(o: any, p: string[]): any { return p.reduce((x, k) => (x && typeof x === "object" ? x[k] : undefined), o); }
function deepSet(o: any, p: string[], v: any) {
  let cur = o;
  for (let i = 0; i < p.length - 1; i++) {
    const k = p[i];
    if (!cur[k] || typeof cur[k] !== "object") cur[k] = {};
    cur = cur[k];
  }
  cur[p[p.length - 1]] = v;
}
function applyIgnorePaths(tgt: Record<string, any>, src: Record<string, any> | undefined, paths: string[]) {
  if (!src) return;
  for (const p of paths) {
    const seg  = p.split(".");
    const prev = deepGet(src, seg);
    if (prev !== undefined) deepSet(tgt, seg, prev);
  }
}

/*────────── BLOCK cache ───────*/
const blkCache = new Map<string, Promise<Record<string, number | undefined>>>();
function blocks(ts: number, chs: string[]) {
  const k = `${ts}/${chs.join(",")}`;
  if (!blkCache.has(k))
    blkCache.set(
      k,
      (async () => {
        const r = await Promise.all(
          chs.map(async (c) => {
            try {
              const b = await getBlocks(ts, [c]);
              return { c, b: b.chainBlocks?.[c] };
            } catch {
              return { c, b: undefined };
            }
          })
        );
        return r.reduce(
          (m, { c, b }) => ({ ...m, [c]: b }),
          {} as Record<string, number | undefined>
        );
      })()
    );
  return blkCache.get(k)!;
}

/*────────── NORMALISE snap ───────*/
function normalise(raw: Snap, pt: string): Chain {
  const out: Chain = {
    minted:      { [pt]: raw.minted },
    unreleased:  { [pt]: raw.unreleased },
    circulating: { [pt]: 0 },
    bridgedTo:   { peggedUSD: 0, bridges: {} },
  };
  for (const [prov, dests] of Object.entries(raw.bridgesOut)) {
    const pMap: Record<string, { amount: number }> = {};
    for (const [dstKey, amt] of Object.entries(dests)) {
      const amount = Number(amt);
      if (amount <= 0 || Number.isNaN(amount)) continue;
      pMap[lower(dstKey)] = { amount };
      out.bridgedTo.peggedUSD += amount;
    }
    if (Object.keys(pMap).length) out.bridgedTo.bridges[prov] = pMap;
  }
  return out;
}

/*────────── mergeChain ───────*/
function mergeChain(prev: Chain, snap: Chain, pt: string): Chain {
  const res: Chain = structuredClone(prev);

  if (snap.minted[pt]     !== 0) res.minted[pt]     = snap.minted[pt];
  if (snap.unreleased[pt] !== 0) res.unreleased[pt] = snap.unreleased[pt];

  for (const [prov, dests] of Object.entries(snap.bridgedTo.bridges))
    res.bridgedTo.bridges[prov] = structuredClone(dests);

  return res;
}
function isAllZero(c: Chain, pt: string) {
  return (
    c.minted[pt] === 0 &&
    c.unreleased[pt] === 0 &&
    c.circulating[pt] === 0 &&
    c.bridgedTo.peggedUSD === 0 &&
    !Object.keys(c.bridgedTo.bridges).length
  );
}

/*────────── STANDARDISE ───────*/
function standardise(rec: DayRec, pt: string) {
  for (const key of Object.keys(rec)) {
    if (["PK", "SK", "totalCirculating"].includes(key)) continue;
    const canon = canonicalChain(key);
    if (canon !== key) {
      if (!rec[canon]) rec[canon] = rec[key];
      else             rec[canon] = mergeChain(rec[canon], rec[key], pt);
      delete rec[key];
    }
  }

  const meta   = ["PK", "SK", "totalCirculating"];
  const chains = Object.keys(rec).filter((k) => !meta.includes(k));

  const inbound:  Record<string, number> = {};
  const outbound: Record<string, number> = {};
  const mirror:   Record<string, Record<string, number>> = {};
  chains.forEach((c) => { inbound[c] = 0; outbound[c] = 0; mirror[c] = {}; });

  for (const dst of chains) {
    let tot = 0;
    for (const prov of Object.values(rec[dst].bridgedTo.bridges) as any[]) {
      for (const [srcRaw, { amount }] of Object.entries(prov) as any) {
        const src = canonical(srcRaw);
        inbound[dst]  += amount;
        outbound[src] += amount;
        tot           += amount;
        mirror[dst][src] = (mirror[dst][src] ?? 0) + amount;
      }
    }
    rec[dst].bridgedTo.peggedUSD = tot;
  }

  for (const ch of chains) {
    const n = rec[ch] as Chain;

    for (const k of Object.keys(n))
      if (!["minted", "unreleased", "circulating", "bridgedTo"].includes(k))
        delete (n as any)[k];

    for (const [srcRaw, amt] of Object.entries(mirror[ch]))
      n[canonical(srcRaw)] = { [pt]: amt };

    const unreleasedUsed =
      ch === "tron"
        ? Math.max(0, n.unreleased[pt] - outbound[ch])
        : n.unreleased[pt];

    n.circulating[pt] =
      n.minted[pt] - unreleasedUsed + inbound[ch] - outbound[ch];
  }

  const total = chains.reduce((s, c) => s + rec[c].circulating[pt], 0);
  rec.totalCirculating = {
    circulating: { [pt]: total },
    unreleased : { [pt]: 0     },
  };
}

/*──── UPGRADE legacy ───*/
function upgrade(rec: DayRec, pt: string): DayRec {
  const meta   = ["PK", "SK", "totalCirculating"];
  const chains = Object.keys(rec).filter((k) => !meta.includes(k));
  for (const ch of chains) {
    const n = rec[ch] as Chain;
    n.minted = { [pt]: Number((n.minted as any)?.[pt] ?? n.minted ?? 0) };
    n.unreleased = { [pt]: Number((n.unreleased as any)?.[pt] ?? n.unreleased ?? 0) };
    delete (n as any).minted?.bridges;
    delete (n as any).unreleased?.bridges;

    const clean: Record<string, Record<string, { amount: number }>> = {};
    n.bridgedTo ??= { peggedUSD: 0, bridges: {} };

    for (const [prov, dests] of Object.entries(n.bridgedTo.bridges ?? {})) {
      const pMap: Record<string, { amount: number }> = {};
      for (const [k, v] of Object.entries(dests as Record<string, any>)) {
        if (k === "source") continue;
        const o   = typeof v === "number" ? { amount: v } : v ?? {};
        const amt = Number(o.amount ?? o[pt] ?? o.peggedUSD ?? 0);
        if (amt <= 0 || Number.isNaN(amt)) continue;
        const dst = k === "amount"
          ? canonical(String(o.source ?? "ethereum"))
          : canonical(k);
        pMap[dst] = { amount: amt };
      }
      if (Object.keys(pMap).length) clean[prov] = pMap;
    }
    n.bridgedTo.bridges = clean;
    n.bridgedTo.peggedUSD = 0;
  }
  standardise(rec, pt);
  return rec;
}

/*────────────────── MAIN ─────────────────*/
(async () => {
  console.log(chalk.bold(`Starting run${apply ? " with --apply" : " (dry-run)"} …`));
  if (ignorePaths.length) console.log(chalk.blue(`Ignoring paths: ${ignorePaths.join(", ")}`));

  const meta      = (peggedData as any[]).find((m) => m.gecko_id === adapterName)!;
  const pt        = meta.pegType || "peggedUSD";
  const adapter   = importAdapter(meta);
  const stamps    = tsRange(start, end);
  const adapterId = meta.id;

  const avail = Object.keys(adapter).map(lower);
  const back  = avail.filter((c) => backfill[canonicalChain(c)]! <= start);

  console.log(chalk.bold("\n⎯⎯⎯ Available ⎯⎯⎯"));
  console.log(chalk.cyan(avail.join(", ")));
  console.log(chalk.bold("\n⎯⎯⎯ Backfillable ⎯⎯⎯"));
  console.log(chalk.cyan(back.join(", ")));
  if (!back.length) {
    console.log(chalk.red("No backfillable chain"));
    rl.close();
    process.exit(0);
  }

  let selected: string[] = [];
  let dyn = false;
  while (true) {
    const a = await ask(chalk.yellow("\nChains? ('all' or list) "));
    if (a.toLowerCase() === "all") {
      dyn = true; selected = back; break;
    }
    const list = a.split(",").map((s) => lower(s.trim())).filter(Boolean);
    const bad  = list.filter((c) => !back.includes(c));
    if (bad.length) { console.log(chalk.red(`Invalid: ${bad.join(", ")}`)); continue; }
    selected = list; break;
  }

  for (const ts of stamps) {
    console.log(chalk.magenta(`\n—— ${ts} (${new Date(ts * 1e3).toISOString().slice(0, 10)}) ——`));

    const chainsRaw = dyn
      ? Object.keys(adapter).filter((c) => backfill[canonicalChain(lower(c))]! <= ts)
      : selected;
    const key = { PK: `dailyPeggedBalances#${adapterId}`, SK: ts };

    const prevRaw = (await db.get({ TableName: tableName, Key: key }).promise()).Item as DayRec | undefined;
    const prev    = prevRaw ? upgrade(prevRaw, pt) : undefined;

    const snaps: Record<string, Chain> = {};
    const errs : string[] = [];

    await Promise.all(
      chainsRaw.map(async (raw) => {
        try {
          const blk = await blocks(ts, [raw]);
          const api = new sdk.ChainApi({ chain: raw, timestamp: ts, block: blk[raw] });
          const snap: Snap = { minted: 0, unreleased: 0, bridgesOut: {} };

          /* minted / unreleased */
          for (const meth of ["minted", "unreleased"] as const) {
            const fn = adapter[raw]?.[meth];
            if (!fn) continue;
            try {
              const f   = typeof (fn as any).then === "function" ? await fn : fn;
              const val = (await f(api)) as number | Pegged;
              snap[meth] = typeof val === "number" ? val : (val as Pegged)[pt] ?? 0;
            } catch (e: any) { errs.push(`${raw}.${meth}: ${e.message || e}`); }
          }

          /* bridges */
          for (const [meth, fnOr] of Object.entries(adapter[raw] ?? {})) {
            if (["minted", "unreleased"].includes(meth)) continue;
            const fn = typeof (fnOr as any).then === "function" ? await fnOr : fnOr;
            try {
              const out = typeof fn === "function" ? await fn(api) : undefined;
              if (!out || typeof out !== "object" || !("bridges" in out)) continue;
              const bridges = out.bridges as Record<string, Record<string, { amount: number }>>;
              for (const [prov, dests] of Object.entries(bridges)) {
                snap.bridgesOut[prov] ??= {};
                for (const [dst, { amount }] of Object.entries(dests))
                  snap.bridgesOut[prov][lower(dst)] = amount;
              }
            } catch (e: any) { errs.push(`${raw}.${meth}: ${e.message || e}`); }
          }

          const canon = canonicalChain(raw);
          snaps[canon] = normalise(snap, pt);
          if (isAllZero(snaps[canon], pt)) delete snaps[canon];
        } catch (e: any) { errs.push(`${raw}: ${e.message || e}`); }
      })
    );

    /* erreurs */
    if (errs.length) {
      console.log(chalk.red(`❌ ${errs.length} errors:`));
      errs.forEach((e) => console.log("  -", e));
      const goOn = (await askYN(chalk.yellow("Continue despite errors ? (y/N) "))).toLowerCase() === "y";
      if (!goOn) { await wait(sleepMs); continue; }
    }
    if (!Object.keys(snaps).length) { console.log(chalk.blue("Nothing to store")); await wait(sleepMs); continue; }

    const rec: DayRec = prev ? structuredClone(prev) : { PK: key.PK, SK: key.SK };
    for (const [c, s] of Object.entries(snaps))
      rec[c] = prev && prev[c] ? mergeChain(prev[c], s, pt) : s;

    standardise(rec, pt);
    applyIgnorePaths(rec, prev, ignorePaths);

    /*──────── Δ chains ───────*/
    console.log(chalk.bold("\nΔ per chain (circulating)"));
    Object.keys(rec)
      .filter((k) => !["PK", "SK", "totalCirculating"].includes(k))
      .forEach((c) => {
        const o = prev?.[c]?.circulating?.[pt] || 0;
        const n = rec[c].circulating[pt];
        const d = n - o;
        if (d !== 0)
          console.log(
            `${c.padEnd(12)} : ${o.toLocaleString()} → ${n.toLocaleString()} ` +
            chalk[d > 0 ? "green" : "red"](`(${d > 0 ? "+" : ""}${d.toLocaleString()})`)
          );
      });

    /*──────── Preview + DIFF ───────*/
    const show = (await askYN(chalk.yellow("Show full merged object? (y/N) "))).toLowerCase() === "y";
    if (show) {
      // console.log(JSON.stringify(rec, null, 2));
      if (prev) {
        const changes = diff(prev, rec) as Diff<any, any>[] | undefined;
        if (changes) {
          console.log(chalk.bold("\n⎯⎯⎯ DIFF ⎯⎯⎯"));
          for (const d of changes) {
            const loc = d.path?.join(".") ?? "";
            const lhs = JSON.stringify((d as any).lhs);
            const rhs = JSON.stringify((d as any).rhs);
            if (d.kind === "E") console.log(`${chalk.cyan(loc)}: ${chalk.red(lhs)} → ${chalk.green(rhs)}`);
            if (d.kind === "N") console.log(`${chalk.cyan(loc)} added:\n${chalk.green(rhs)}`);
            if (d.kind === "D") console.log(`${chalk.cyan(loc)} removed:\n${chalk.red(lhs)}`);
          }
        }
      }
    }

    /*──────── warden totalCirculating ───────*/
    if (prev) {
      const prevTot = prev.totalCirculating.circulating[pt];
      const newTot  = rec.totalCirculating.circulating[pt];
      const diffAbs = Math.abs(newTot - prevTot);
      const diffPct = prevTot ? diffAbs / prevTot : 0;
      if (newTot < prevTot) {
        rec.totalCirculating.circulating[pt] = prevTot;
      } else if (diffPct > THRESHOLD) {
        console.log(chalk.bold("\nΔ totalCirculating"));
        console.log(chalk.red(`${prevTot.toLocaleString()} → ${newTot.toLocaleString()}`));
        const ok = (await askYNmanual(chalk.yellow("⚠  Recalculated totalCirculating is HIGHER. Accept? (y/N) "))).trim().toLowerCase() === "y";
        if (!ok) { await wait(sleepMs); continue; }
      }
    }

    /*──────── write ───────*/
    if (apply) {
      const yes = (await askYN(chalk.yellow("Apply to DynamoDB? (y/N) "))).toLowerCase() === "y";
      if (yes) {
        try {
          await db.put({ TableName: tableName, Item: rec }).promise();
          console.log(chalk.green("✔ applied"));
        } catch (e) {
          console.error(chalk.red("❌ write failed"), e);
          const cont = (await askYN(chalk.yellow("Continue next? (y/N) "))).toLowerCase() === "y";
          if (!cont) { rl.close(); process.exit(1); }
        }
      } else console.log(chalk.blue("Push skipped"));
    }

    const cont = (await askYN(chalk.yellow("Continue to next timestamp? (y/N) "))).toLowerCase() === "y";
    if (!cont) { console.log(chalk.blue("Process interrupted")); rl.close(); process.exit(0); }

    await wait(sleepMs);
  }

  console.log(chalk.bold("\nDone."));
  rl.close();
  process.exit(0);
})().catch((e) => {
  console.error(chalk.red("Fatal error"), e);
  process.exit(1);
});


/**
 * 
 * npx ts-node --transpile-only refill.ts --adapter=societe-generale-forge-eurcv --start=1698278400 --end=1744243200  --table=prod-stablecoins-table --region=eu-central-1 --sleep=1000 --yes --apply
 * 
 */