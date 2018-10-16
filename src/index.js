'use strict'

const EventEmitter = require('events')
const assert = require('assert')

const mapKeys = require('lodash.mapkeys')
const camelCase = require('lodash.camelcase')

const log = require('debug')('kitsunet:slice-tracker')

const DEFAULT_TOPIC = `kitsunet:slice`

function normalizeSlice (slice) {
  mapKeys(slice, (_, key) => {
    return camelCase(key)
  })
}

class KitsunetSliceTracker extends EventEmitter {
  constructor ({ node, blockTracker, topic }) {
    super()

    assert(node, 'node is required!')
    assert(node.multicast, 'node needs multicast!')
    assert(blockTracker, 'blockTracker is required!')

    this.node = node
    this.blockTracker = blockTracker
    this.topic = topic || DEFAULT_TOPIC
    this.started = false

    this.peerSlices = new Map()
    this.slices = new Map()

    this.node.multicast.addFrwdHooks(this.topic, [(peer, msg, cb) => {
      let slice = null
      try {
        slice = normalizeSlice(JSON.parse(msg.data.toString()))
        if (!slice) {
          return cb(new Error(`No slice in message!`))
        }
      } catch (err) {
        log(err)
        return cb(err)
      }

      const peerSlices = this.peerSlices.has(peer.info.id.toB58String()) || new Set()
      if (peerSlices.has(slice.sliceId)) {
        const msg = `already forwarded to peer, skipping slice ${slice.sliceId}`
        log(msg)
        return cb(msg)
      }
      peerSlices.add(slice.sliceId)
      return true
    }])
  }

  async getLatestSlice (path, depth) {
    const block = await this.blockTracker.getLatestBlock()
    return this.getSliceForBlock(path, depth, block)
  }

  async getSliceForBlock (path, depth, block) {
    const sliceId = `${path}-${depth}-${block.stateRoot}`
    const slices = this.slices(sliceId)
    if (slices.has(block.stateRoot)) return slices.get(sliceId)
    return Promise(resolve => this.on(`latest:${path}-${depth}`, resolve))
      .then(() => slices.get(sliceId))
  }

  _handler (msg) {
    const data = msg.data.toString()
    try {
      const slice = normalizeSlice(JSON.parse(data))
      this.slices.set(slice.sliceId, slice)
      const [path, depth] = slice.sliceId.split('-')
      this.emit(`latest:${path}-${depth}`, slice)
      this.emit(`slice:${path}-${depth}`, slice)
    } catch (err) {
      log(err)
    }
  }

  start () {
    if (!this.started) {
      this.node.multicast.subscribe(this.topic, this._handler.bind(this), () => { })
    }
  }

  stop () {
    if (this.started) {
      this.node.multicast.unsubscribe(this.topic, this._handler.bind(this), () => { })
    }
  }
}

module.exports = KitsunetSliceTracker
