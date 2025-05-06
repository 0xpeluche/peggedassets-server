process.env.SKIP_RPC_CHECK = 'true'

require('dotenv').config()
import * as sdk from '@defillama/sdk'
import { getBlocks } from '@defillama/sdk/build/computeTVL/blocks'
import { getLatestBlock } from '@defillama/sdk/build/util/index'
import path from 'path'
import { PeggedAssetIssuance, PeggedTokenBalance } from '../../types'
import { PeggedIssuanceAdapter } from './peggedAsset.type'

const { humanizeNumber } = require('@defillama/sdk/build/computeTVL/humanizeNumber')
const chainList = require('./helper/chains.json')

const errorString = '------ ERROR ------'

const MAX_BLOCK_TIME_DIFF = 24 * 3600 // 24h

type ChainBlocks = Record<string, number>
type BridgeMapping = Record<string, PeggedTokenBalance[]>
const pegTypes = ['peggedUSD', 'peggedEUR', 'peggedVAR']

async function getLatestBlockRetry(chain: string) {
  for (let i = 0; i < 5; i++) {
    try { return await getLatestBlock(chain) }
    catch (e) { if (i === 4) throw new Error(`Couldn't get block for ${chain}: ${e}`) }
  }
}

async function getPeggedAsset(
  _unixTimestamp: number,
  ethBlock: number | undefined,
  chainBlocks: ChainBlocks,
  peggedBalances: PeggedAssetIssuance,
  chain: string,
  issuanceType: string,
  issuanceFunction: any,
  pegType: string,
  bridgedFromMapping: BridgeMapping = {}
) {
  peggedBalances[chain] = peggedBalances[chain] || {}
  const to = setTimeout(
    () => console.warn(`Issuance fn timeout on ${chain}/${issuanceType}`),
    60_000
  )

  const api = new sdk.ChainApi({ chain, block: chainBlocks[chain], timestamp: _unixTimestamp })
  const balance = (await issuanceFunction(api, ethBlock, chainBlocks)) as PeggedTokenBalance
  clearTimeout(to)

  if (!balance || Object.keys(balance).length === 0) {
    peggedBalances[chain][issuanceType] = { [pegType]: 0 }
    return
  }
  if (typeof balance[pegType] !== 'number' || Number.isNaN(balance[pegType])) {
    throw new Error(`Invalid ${pegType} on ${chain}/${issuanceType}: ${balance[pegType]}`)
  }

  if (!(balance as any).bridges) console.warn(`${errorString}\nNo bridges data on ${chain}`)
  peggedBalances[chain][issuanceType] = balance
  if (issuanceType !== 'minted' && issuanceType !== 'unreleased') {
    bridgedFromMapping[issuanceType] = bridgedFromMapping[issuanceType] || []
    bridgedFromMapping[issuanceType].push(balance)
  }
}

async function calcCirculating(
  peggedBalances: PeggedAssetIssuance,
  bridgedFromMapping: BridgeMapping,
  pegType: string
) {
  // Per-chain
  await Promise.all(
    Object.keys(peggedBalances).map(async (chain) => {
      const data = peggedBalances[chain]
      if (typeof data !== 'object') return
      const circ: PeggedTokenBalance = { [pegType]: 0 }

      // Sum minted vs unreleased
      Object.entries(data).forEach(([type, obj]: [string, PeggedTokenBalance]) => {
        const v = obj[pegType] ?? 0
        circ[pegType]! += type === 'unreleased' ? -v : v
      })

      // Subtract bridged-from
      ;(bridgedFromMapping[chain] || []).forEach((b: PeggedTokenBalance) => {
        const v = b[pegType] ?? 0
        if (v && circ[pegType] !== 0) circ[pegType]! -= v
      })

      // Prevent negatives
      if (circ[pegType]! < 0) {
        console.warn(`Negative circulating on ${chain} (${circ[pegType]!}), resetting to 0.`)
        circ[pegType] = 0
      }

      peggedBalances[chain].circulating = circ
    })
  )

  // Totals
  peggedBalances.totalCirculating = {
    circulating: { [pegType]: 0 },
    unreleased: { [pegType]: 0 },
  } as any

  Object.entries(peggedBalances).forEach(([chain, data]) => {
    if (chain === 'totalCirculating' || typeof data !== 'object') return
    const c = data.circulating?.[pegType] ?? 0
    const u = data.unreleased?.[pegType] ?? 0
    peggedBalances.totalCirculating.circulating[pegType]! += c
    peggedBalances.totalCirculating.unreleased[pegType]! += u
  })
}


if (process.argv.length < 3) {
  console.error('Usage: npx ts-node test <adapter> <pegType> [timestamp]')
  process.exit(1)
}

const passedFile = path.resolve(process.cwd(), process.argv[2])
const dummyFn = () => ({})
const INTERNAL_CACHE_FILE = 'pegged-assets-cache/sdk-cache.json'

;(async () => {
  let adapter: PeggedIssuanceAdapter
  try { adapter = require(passedFile) }
  catch (e) { console.error('Cannot load adapter:', e); process.exit(1) }
  const module = adapter.default
  const chains = Object.keys(module).filter(c => !['minted','unreleased'].includes(c))
  checkExportKeys(passedFile, chains)

  let unixTimestamp = Math.floor(Date.now()/1000) - 60
  const pegType = process.argv[3] ?? 'peggedUSD'
  const passedTs = process.argv[4]
  if (passedTs) {
    unixTimestamp = Number(passedTs)
    if (isNaN(unixTimestamp) || unixTimestamp < 1e9) throw new Error('Invalid timestamp')
  }

  if (!chains.includes('ethereum')) chains.push('ethereum')
  let chainBlocks: ChainBlocks = {}

  if (passedTs) {
    const { chainBlocks: blocks } = await getBlocks(unixTimestamp, chains);
    chainBlocks = blocks;
  }

  const ethBlock = chainBlocks.ethereum
  const peggedBalances: PeggedAssetIssuance = {} as any
  const bridgedFromMapping: BridgeMapping = {}

  await initializeSdkInternalCache()

  // run adapters
  await Promise.all(Object.entries(module).map(async ([chain, issuances]) => {
    if (passedTs && chainBlocks[chain] === undefined) {
      console.warn(`Skipping ${chain}; no valid block at ${unixTimestamp}`)
      return
    }
    if (typeof issuances !== 'object' || !issuances) return
    if (!issuances.minted) issuances.minted = dummyFn
    if (!issuances.unreleased) issuances.unreleased = dummyFn
    if (chain in issuances) throw new Error(`${chain} bridged to itself`)

    await Promise.all(Object.entries(issuances).map(async ([type, p]) => {
      const fn = await p
      if (typeof fn !== 'function') return
      try {
        await getPeggedAsset(
          unixTimestamp, ethBlock, chainBlocks,
          peggedBalances, chain, type, fn, pegType, bridgedFromMapping
        )
      } catch (e) {
        console.error(`Error on ${chain}:${type}`, e)
      }
    }))
  }))

  await calcCirculating(peggedBalances, bridgedFromMapping, pegType)

  const tot = peggedBalances.totalCirculating.circulating[pegType]
  if (typeof tot !== 'number') throw new Error('No totalCirculating')
  if (tot > 1e12) throw new Error('Unrealistic totalCirculating')
  if (tot === 0) throw new Error('Zero totalCirculating')

  // PRINT
  Object.entries(peggedBalances).forEach(([chain,data]) => {
    if (chain==='totalCirculating'||typeof data!=='object') return
    console.log(`--- ${chain} ---`)
    Object.entries(data)
      .sort((a,b)=>(b[1][pegType]||0)-(a[1][pegType]||0))
      .forEach(([t,b])=>{
        console.log(t.padEnd(25,' '), humanizeNumber(b[pegType]))
      })
  })
  console.log('------ Total Circulating ------')
  Object.entries(peggedBalances.totalCirculating).forEach(([t,b])=>{
    console.log(`Total ${t}`.padEnd(25,' '), humanizeNumber(b[pegType]))
  })

  await saveSdkInternalCache()
  process.exit(0)
})()

// UTILITIES
function checkExportKeys(_fp: string, chains: string[]) {
  const u = chains.filter(c=>!chainList.includes(c))
  if (u.length) { console.error(`${errorString}\nUnknown chains: ${u.join(', ')}`); process.exit(1) }
}
function handleError(e:any){ console.error(errorString,e); process.exit(1) }
process.on('unhandledRejection', handleError)
process.on('uncaughtException', handleError)

async function initializeSdkInternalCache() {
  const ONE_MONTH = 60*60*24*30
  let cache = await sdk.cache.readCache(INTERNAL_CACHE_FILE)
  if (!cache||!cache.startTime||Date.now()/1000-cache.startTime>ONE_MONTH) {
    cache={startTime:Math.floor(Date.now()/1000)}
    await sdk.cache.writeCache(INTERNAL_CACHE_FILE,cache)
  }
  ;(sdk as any).sdkCache.startCache(cache)
}

async function saveSdkInternalCache() {
  await sdk.cache.writeCache(INTERNAL_CACHE_FILE,(sdk as any).sdkCache.retriveCache())
}
