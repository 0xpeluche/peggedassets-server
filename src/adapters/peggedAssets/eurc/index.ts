const sdk = require("@defillama/sdk");
import { sumSingleBalance } from "../helper/generalUtil";
import { bridgedSupply, chainMinted, chainUnreleased, solanaMintedOrBridged, supplyInEthereumBridge } from "../helper/getSupply";
import {
  Balances,
  ChainBlocks,
  ChainContracts,
  PeggedIssuanceAdapter,
} from "../peggedAsset.type";
const axios = require("axios");
const retry = require("async-retry");

import { getTokenBalance as solanaGetTokenBalance } from "../helper/solana";

const chainContracts: ChainContracts = {
  ethereum: {
    issued: ["0x1abaea1f7c830bd89acc67ec4af516284b1bc33c"],
    unreleased: ["0x55fe002aeff02f77364de339a1292923a15844b8"],
  },
  polygon: {
    bridgedFromETH: ["0x8a037dbcA8134FFc72C362e394e35E0Cad618F85"],
  },
  avax: {
    issued: ["0xc891eb4cbdeff6e073e859e987815ed1505c2acd"],
  },
  base: {
    issued: ["0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42"],
  },
  solana: {
    issued: ["HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr"],
    unreleased: ["7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE"], 
  },
  sonic: {
    bridgedFromETH: ["0xe715cbA7B5cCb33790ceBFF1436809d36cb17E57"],
  },
};

async function solanaUnreleased() {
  return async function (
    _timestamp: number,
    _ethBlock: number,
    _chainBlocks: ChainBlocks
  ) {
    let balances = {} as Balances;
    const unreleased = await solanaGetTokenBalance(
      chainContracts["solana"].issued[0],
      chainContracts["solana"].unreleased[0]
    );
    sumSingleBalance(balances, "peggedEUR", unreleased);
    return balances;
  };
}

async function circleAPIChainMinted(chain: string) {
  return async function (
    _timestamp: number,
    _ethBlock: number,
    _chainBlocks: ChainBlocks
  ) {
    let balances = {} as Balances;
    const issuance = await retry(
      async (_bail: any) =>
        await axios.get("https://api.circle.com/v1/stablecoins")
    );
    const eurcData = issuance.data.data.filter(
      (obj: any) => obj.symbol === "EUROC"
    );
    const filteredChainsData = await eurcData[0].chains.filter(
      (obj: any) => obj.chain === chain
    );
    const supply = parseInt(filteredChainsData[0].amount);
    sumSingleBalance(balances, "peggedEUR", supply, "issued", false);
    return balances;
  };
}

const adapter: PeggedIssuanceAdapter = {
  ethereum: {
    minted: chainMinted(chainContracts.ethereum.issued, 6, "peggedEUR"),
    unreleased: chainUnreleased(chainContracts.ethereum.issued, 6, chainContracts.ethereum.unreleased[0], "peggedEUR"),
  },
  polygon: {
    ethereum: bridgedSupply(
      "polygon",
      6,
      chainContracts.polygon.bridgedFromETH,
      "polygon",
      "Ethereum",
      "peggedEUR"
    ),
  },
  avax: {
    minted: chainMinted(chainContracts.avax.issued, 6, "peggedEUR"),
  },
  stellar: {
    minted: circleAPIChainMinted("XLM"),
  },
  base: {
    minted: chainMinted(chainContracts.base.issued, 6, "peggedEUR"),
  },
  solana: {
    minted: solanaMintedOrBridged(chainContracts.solana.issued, "peggedEUR"),
    unreleased: solanaUnreleased(),
  },
  icp: {
    ethereum: supplyInEthereumBridge(
      '0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c', 
      '0xb25eA1D493B49a1DeD42aC5B1208cC618f9A9B80', 
      6, 
      "peggedEUR"
    ),
  },
  sonic: {
    ethereum: bridgedSupply(
      "sonic",
      6,
      chainContracts.sonic.bridgedFromETH,
      "sonic",
      "Ethereum",
      "peggedEUR"
    ),
  },
};

export default adapter