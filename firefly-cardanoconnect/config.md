## api

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|address|Listener address for API|`string`|`0.0.0.0`
|port|Listener port for API|`int`|`5018`

## connector

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|signerUrl|The URL of the firefly-cardanosigner instance to call|`string`|`http://127.0.0.1:8555`

## connector.blockchain

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|socket|Path to the socket of a Cardano node|`string`|
|blockfrostKey|Blockfrost API key|`string`|
|network|The network to connect to|'mainnet', 'preview', or 'preprod'|`mainnet`
|networkMagic|An optional magic number|`int`|depends on `network`
|genesisHash|The hash of the genesis block|`string`|depends on `network`

## contracts
|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|componentsPath|Path on-disk to store the wasm components which define contracts|`string`||
|storesPath|Path on-disk to store persistent data for contracts|`string`||
|cacheSize|The amount of memory (in bytes) used to cache data|`number`|0|

## http

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|connectionTimeout|Timeout to connect to another server|`30s`
|requestTimeout|Timeout to make an HTTP request|`30s`

## log

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|level|The log level - error, warn, info, debug, trace|`string`|`info`

## persistence

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|type|The type of persistence to use|'mock' or 'sqlite'|`mock`

## persistence.sqlite

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|path|The path for the sqlite file|`string`|
