import * as sdk from "@defillama/sdk";
import { PeggedAsset } from "../../peggedData/peggedData";
import {
  BridgeBalances,
  PeggedAssetIssuance,
  PeggedTokenBalance,
} from "../../types";
import { getCurrentUnixTimestamp } from "../../utils/date";
import { getClosestSnapshotForChain } from "../../utils/extrapolatedCacheFallback";
import {
  dailyPeggedBalances,
  hourlyPeggedBalances,
} from "../utils/getLastRecord";
import { executeAndIgnoreErrors } from "./errorDb";
import storeNewPeggedBalances from "./storeNewPeggedBalances";

type ChainBlocks = {
  [chain: string]: number;
};

type BridgeMapping = {
  [chain: string]: PeggedTokenBalance[];
};

type EmptyObject = { [key: string]: undefined };

async function getPeggedAsset(
  api: sdk.ChainApi,
  ethBlock: number | undefined,
  chainBlocks: ChainBlocks | undefined,
  peggedAsset: PeggedAsset,
  peggedBalances: PeggedAssetIssuance,
  chain: string,
  issuanceType: string,
  issuanceFunction: any,
  pegType: string,
  bridgedFromMapping: BridgeMapping = {},
  maxRetries: number,
  extrapolationMetadata?: { extrapolated: boolean; extrapolatedChains: Array<{ chain: string; timestamp: number }> }
) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      peggedBalances[chain] = peggedBalances[chain] || {};
      const balance = (await issuanceFunction(api, ethBlock, chainBlocks)) as PeggedTokenBalance;
      if (balance && Object.keys(balance).length === 0) {
        peggedBalances[chain][issuanceType] = { [pegType]: 0 };
        return;
      }
      if (!balance) {
        throw new Error(
          `Could not get pegged balance for ${peggedAsset.name} on chain ${chain}`
        );
      }
      if (
        typeof balance[pegType] !== "number" ||
        Number.isNaN(balance[pegType])
      ) {
        throw new Error(
          `Pegged balance for ${peggedAsset.name} is not a number, instead it is ${balance[pegType]}`
        );
      }

      peggedBalances[chain][issuanceType] = balance;
      if (issuanceType !== "minted" && issuanceType !== "unreleased") {
        // issuanceType must be a chain as key on bridgedFromMapping, but I check for that when testing adapters.
        bridgedFromMapping[issuanceType] =
          bridgedFromMapping[issuanceType] || [];
        bridgedFromMapping[issuanceType].push(balance);
      }
      return;
    } catch (e) {
      if (i >= maxRetries - 1) {
        console.error(
          `Getting circulating for ${peggedAsset.name} on chain ${chain} failed.`,
          e
        );
        
        try {
          const snap = await getClosestSnapshotForChain(
            peggedAsset.gecko_id,
            chain,
            getCurrentUnixTimestamp(),
          );
          
          if (snap && snap.snapshot && typeof snap.snapshot === 'object') {
            console.log(`Using cache fallback for ${peggedAsset.name} on chain ${chain}`);
            
            if (extrapolationMetadata) {
              extrapolationMetadata.extrapolated = true;
              if (!extrapolationMetadata.extrapolatedChains.find(ec => ec.chain === chain)) {
                extrapolationMetadata.extrapolatedChains.push({ 
                  chain, 
                  timestamp: snap.timestamp 
                });
              }
            }
            
            const bridgedTo = snap.snapshot.bridgedTo;
            if (bridgedTo && typeof bridgedTo === 'object') {
              if (issuanceType === 'minted' || issuanceType === 'unreleased') {
                const value = bridgedTo[issuanceType]?.[pegType];
                if (value != null) {
                  peggedBalances[chain][issuanceType] = { [pegType]: value };
                  return;
                }
              } else {
                const bridges = bridgedTo.bridges;
                if (bridges && typeof bridges === 'object') {
                  let totalAmount = 0;
                  for (const bridgeProtocol of Object.keys(bridges)) {
                    const protocolData = bridges[bridgeProtocol];
                    if (protocolData && typeof protocolData === 'object') {
                      for (const destChain of Object.keys(protocolData)) {
                        if (destChain.toLowerCase() === issuanceType.toLowerCase()) {
                          const amount = protocolData[destChain]?.amount;
                          if (amount != null) {
                            totalAmount += amount;
                          }
                        }
                      }
                    }
                  }
                  if (totalAmount > 0) {
                    peggedBalances[chain][issuanceType] = { [pegType]: totalAmount };
                    return;
                  }
                }
              }
            }
          }
        } catch (cacheError) {
          console.error(`Cache fallback also failed for ${peggedAsset.name} on chain ${chain}:`, cacheError);
        }
        
        executeAndIgnoreErrors("INSERT INTO `errors2` VALUES (?, ?, ?, ?)", [
          getCurrentUnixTimestamp(),
          peggedAsset.gecko_id,
          chain,
          String(e),
        ]);
        peggedBalances[chain][issuanceType] = { [pegType]: null };
      } else {
        console.error(peggedAsset.name, e);
        continue;
      }
    }
  }
}

function mergeBridges(
  bridgeBalances: BridgeBalances | EmptyObject,
  bridgeBalancesToMerge: BridgeBalances
) {
  if (bridgeBalances && Object.keys(bridgeBalances).length === 0) {
    return bridgeBalancesToMerge;
  }
  bridgeBalances = bridgeBalances as BridgeBalances;
  for (let bridgeID in bridgeBalancesToMerge) {
    for (let sourceChain in bridgeBalancesToMerge[bridgeID]) {
      if (bridgeBalances?.[bridgeID]?.[sourceChain]) {
        const bridgeBalance = bridgeBalances[bridgeID][sourceChain].amount ?? 0;
        const bridgeBalanceToMerge =
          bridgeBalancesToMerge[bridgeID][sourceChain].amount;
        if (
          typeof bridgeBalance === "number" &&
          typeof bridgeBalanceToMerge === "number"
        ) {
          bridgeBalances[bridgeID][sourceChain].amount =
            bridgeBalance + bridgeBalanceToMerge;
        } else {
          bridgeBalances[bridgeID][sourceChain].amount = Number(BigInt(bridgeBalance ?? 0) + BigInt(bridgeBalanceToMerge ?? 0));
        }
      } else {
        bridgeBalances[bridgeID] = bridgeBalances[bridgeID] || {};
        bridgeBalances[bridgeID][sourceChain] =
          bridgeBalancesToMerge[bridgeID][sourceChain];
      }
    }
  }
  return bridgeBalances;
}

async function calcCirculating(
  peggedBalances: PeggedAssetIssuance,
  bridgedFromMapping: BridgeMapping,
  peggedAsset: PeggedAsset,
  pegType: string
) {
  let chainCirculatingPromises = Object.keys(peggedBalances).map(
    async (chain) => {
      let circulating: PeggedTokenBalance = { [pegType]: 0 };
      peggedBalances[chain].bridgedTo = {};
      peggedBalances[chain].bridgedTo[pegType] = 0;
      const chainIssuances = peggedBalances[chain];
      Object.entries(chainIssuances).map(
        ([issuanceType, peggedTokenBalance]) => {
          const balance = peggedTokenBalance[pegType];
          const bridges = JSON.parse(
            JSON.stringify(peggedTokenBalance.bridges ?? {})
          );
          if (balance == null) {
            return;
          }
          if (issuanceType === "unreleased") {
            circulating[pegType] = circulating[pegType] || 0;
            circulating[pegType]! -= balance;
          } else {
            if (issuanceType !== "bridgedTo") {
              if (issuanceType !== "minted" && issuanceType !== "circulating") {
                // issuanceType is a chain here
                peggedBalances[chain].bridgedTo[pegType]! += balance;
                if (bridges) {
                  peggedBalances[chain].bridgedTo.bridges = mergeBridges(
                    peggedBalances[chain].bridgedTo.bridges || {},
                    bridges
                  );
                }
              }
              circulating[pegType] = circulating[pegType] || 0;
              circulating[pegType]! += balance; // issuanceType is either "minted" or a chain here
              delete peggedTokenBalance.bridges;
            }
          }
        }
      );
      if (bridgedFromMapping[chain]) {
        bridgedFromMapping[chain].forEach((peggedTokenBalance) => {
          const balance = peggedTokenBalance[pegType];
          if (balance == null || circulating[pegType] === 0) {
            console.error(
              `Null balance or 0 circulating error on chain ${chain}`
            );
            executeAndIgnoreErrors(
              "INSERT INTO `errors2` VALUES (?, ?, ?, ?)",
              [
                getCurrentUnixTimestamp(),
                peggedAsset.gecko_id,
                chain,
                `Null balance or 0 circulating error`,
              ]
            );
            return;
          }
          circulating[pegType]! -= balance;
        });
      }
      if (circulating[pegType]! < 0) {
        executeAndIgnoreErrors("INSERT INTO `errors2` VALUES (?, ?, ?, ?)", [
          getCurrentUnixTimestamp(),
          peggedAsset.gecko_id,
          chain,
          `Pegged asset has negative circulating amount`,
        ]);
        throw new Error(
          `Pegged asset on chain ${chain} has negative circulating amount`
        );
      }
      // Fix this.
      // Rounding down small balances to avoid dealing with scientific floating points. Will be a problem for non-peggedUSD/peggedBTC.
      // Also, 'minted' and 'unreleased' values are also used in frontend, need to deal with those too.
      if (circulating[pegType]! < 10 ** -6) {
        circulating[pegType] = 0;
      }
      peggedBalances[chain].circulating = circulating;
    }
  );
  await Promise.all(chainCirculatingPromises);

  peggedBalances["totalCirculating"] = {};
  peggedBalances["totalCirculating"]["circulating"] = { [pegType]: 0 };
  peggedBalances["totalCirculating"]["unreleased"] = { [pegType]: 0 };
  let peggedTotalPromises = Object.keys(peggedBalances).map((chain) => {
    const circulating = peggedBalances[chain].circulating;
    const unreleased = peggedBalances[chain].unreleased;
    if (chain !== "totalCirculating") {
      peggedBalances["totalCirculating"]["circulating"][pegType]! +=
        circulating[pegType] || 0;
      peggedBalances["totalCirculating"]["unreleased"][pegType]! +=
        unreleased[pegType] || 0;
    }
  });
  await Promise.all(peggedTotalPromises);
}

const timeout = (prom: any, time: number, peggedID: string, chain: string) =>
  Promise.race([prom, new Promise((_r, rej) => setTimeout(rej, time))]).catch(
    async (err) => {
      console.error(
        `Could not store peggedAsset ${peggedID} on chain ${chain}`,
        err
      );
      throw err;
    }
  );

export async function storePeggedAsset(
  unixTimestamp: number,
  ethBlock: number | undefined,
  chainBlocks: ChainBlocks | undefined,
  peggedAsset: PeggedAsset,
  module: any,
  maxRetries: number = 1,
  breakIfIssuanceIsZero: boolean = false
  //storePreviousData: boolean = true,
  //runBeforeStore?: () => Promise<void>
) {
  const pegType = peggedAsset.pegType;
  let peggedBalances: PeggedAssetIssuance = {};
  let bridgedFromMapping: BridgeMapping = {};
  
  const extrapolationMetadata = {
    extrapolated: false,
    extrapolatedChains: [] as Array<{ chain: string; timestamp: number }>
  };
  
  try {
    let peggedBalancesPromises = Object.entries(module).map(
      async ([chain, issuances]) => {
        if (typeof issuances !== "object" || issuances === null) {
          return;
        }
        let peggedChainPromises = Object.entries(issuances).map(
          async ([issuanceType, issuanceFunctionPromise]) => {
            const issuanceFunction = await issuanceFunctionPromise;
            if (typeof issuanceFunction !== "function") {
              return;
            }
            const api = new sdk.ChainApi({ chain, timestamp: unixTimestamp });
            await getPeggedAsset(
              api,
              ethBlock,
              chainBlocks,
              peggedAsset,
              peggedBalances,
              chain,
              issuanceType,
              issuanceFunction,
              pegType,
              bridgedFromMapping,
              maxRetries,
              extrapolationMetadata
            );
          }
        );
        await timeout(
          Promise.all(peggedChainPromises),
          3 * 60 * 1000, // 3 minutes
          peggedAsset.name,
          chain
        );
      }
    );
    await Promise.all(peggedBalancesPromises);
    await calcCirculating(
      peggedBalances,
      bridgedFromMapping,
      peggedAsset,
      pegType
    );

    if (
      typeof peggedBalances.totalCirculating.circulating[pegType] !== "number"
    ) {
      throw new Error(`Pegged asset doesn't have total circulating`);
    }
    if (peggedBalances.totalCirculating.circulating[pegType]! > 100e10) {
      throw new Error(`Pegged asset total circulating is over 1 trillion`);
    }
  } catch (e) {
    console.error(peggedAsset.name, e);
    return;
  }
  if (
    breakIfIssuanceIsZero &&
    peggedBalances.totalCirculating.circulating[pegType] === 0
  ) {
    throw new Error(
      `Returned 0 total circulating at timestamp ${unixTimestamp}`
    );
  }

  try {
    // Checks circuit breakers
    const storeTokensAction = storeNewPeggedBalances(
      peggedAsset,
      unixTimestamp,
      peggedBalances,
      hourlyPeggedBalances,
      dailyPeggedBalances,
      extrapolationMetadata
    );
    await storeTokensAction;
  } catch (e) {
    console.error(peggedAsset.name, e);
    return;
  }

  if (extrapolationMetadata.extrapolated) {
    console.log(`[${peggedAsset.name}] ⚠️  Used cache fallback for some chains:`, extrapolationMetadata.extrapolatedChains.map(ec => ec.chain).join(', '));
  }

  return peggedBalances;
}
