import os from 'node:os'
import fs from 'node:fs'
import path from 'node:path'
import stream from 'node:stream/promises'
import {strict as assert} from 'node:assert'

import {StreamSplitter} from '../models/index.js'
import {Readable} from 'node:stream'

const fileSize = 20_971_520

describe('StreamSplitter', () => {
  it('should buffer chunks until optimal part size', async () => {
    const readStream = fs.createReadStream('../../test/fixtures/test.pdf')
    const optimalChunkSize = 8 * 1024 * 1024
    const parts = [optimalChunkSize, optimalChunkSize, fileSize - optimalChunkSize * 2]
    let offset = 0
    let index = 0
    const splitterStream = new StreamSplitter({
      chunkSize: optimalChunkSize,
      directory: os.tmpdir(),
    }).on('chunkFinished', ({size}) => {
      offset += size
      assert.equal(parts[index], size)
      index++
    })
    await stream.pipeline(readStream, splitterStream)
    assert.equal(offset, fileSize)
  })

  it('should split to multiple chunks when single buffer exceeds chunk size', async () => {
    const optimalChunkSize = 1024
    const expectedChunks = 7

    const readStream = Readable.from([Buffer.alloc(expectedChunks * optimalChunkSize)])

    let chunksStarted = 0
    let chunksFinished = 0
    const splitterStream = new StreamSplitter({
      chunkSize: optimalChunkSize,
      directory: os.tmpdir(),
    })
      .on('chunkStarted', () => {
        chunksStarted++
      })
      .on('chunkFinished', () => {
        chunksFinished++
      })

    await stream.pipeline(readStream, splitterStream)

    assert.equal(chunksStarted, expectedChunks)
    assert.equal(chunksFinished, expectedChunks)
  })
})

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => {
    resolve = done
  })
  return {promise, resolve}
}

describe('StreamSplitter cancellation', () => {
  let directory: string
  const fileSystem = fs.promises
  const open = fileSystem.open
  const appendFile = fileSystem.appendFile

  beforeEach(async () => {
    directory = await fileSystem.mkdtemp(path.join(os.tmpdir(), 'splitter-cancel-'))
  })

  afterEach(async () => {
    fileSystem.open = open
    fileSystem.appendFile = appendFile
    await fileSystem.rm(directory, {recursive: true, force: true})
  })

  it('closes and removes a file opened after destruction starts', async () => {
    const opened = deferred()
    const resume = deferred()
    let handle: fs.promises.FileHandle | undefined
    fileSystem.open = (async (...args: Parameters<typeof open>) => {
      handle = await open(...args)
      opened.resolve()
      await resume.promise
      return handle
    }) as typeof open

    const splitter = new StreamSplitter({chunkSize: 100, directory})
    splitter.on('error', () => {})
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.write(Buffer.from('payload'))
    await opened.promise
    splitter.destroy(new Error('aborted'))
    resume.resolve()
    await closed

    assert.equal(handle?.fd, -1)
    assert.deepEqual(await fileSystem.readdir(directory), [])
  })

  it('closes and removes a file when chunkStarted is interrupted', async () => {
    const started = deferred()
    const resume = deferred()
    const splitter = new StreamSplitter({chunkSize: 100, directory})
    splitter.on('chunkStarted', async () => {
      started.resolve()
      await resume.promise
    })
    splitter.on('error', () => {})
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.write(Buffer.from('payload'))
    await started.promise
    const handle = splitter.fileHandle
    splitter.destroy(new Error('aborted'))
    resume.resolve()
    await closed

    assert.equal(handle?.fd, -1)
    assert.deepEqual(await fileSystem.readdir(directory), [])
  })

  it('waits for a pending write before removing the unfinished chunk', async () => {
    const writing = deferred()
    const resume = deferred()
    fileSystem.appendFile = (async (...args: Parameters<typeof appendFile>) => {
      writing.resolve()
      await resume.promise
      return appendFile(...args)
    }) as typeof appendFile

    const splitter = new StreamSplitter({chunkSize: 100, directory})
    splitter.on('error', () => {})
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.write(Buffer.from('payload'))
    await writing.promise
    const handle = splitter.fileHandle
    splitter.destroy(new Error('aborted'))
    resume.resolve()
    await closed

    assert.equal(handle?.fd, -1)
    assert.deepEqual(await fileSystem.readdir(directory), [])
  })

  it('removes an unfinished chunk on destruction without an error', async () => {
    const splitter = new StreamSplitter({chunkSize: 100, directory})
    await new Promise<void>((resolve, reject) => {
      splitter.write(Buffer.from('payload'), (error) =>
        error ? reject(error) : resolve()
      )
    })
    const handle = splitter.fileHandle
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.destroy()
    await closed

    assert.equal(handle?.fd, -1)
    assert.deepEqual(await fileSystem.readdir(directory), [])
  })

  it('does not remove a chunk handed off while destruction waits', async () => {
    const started = deferred()
    const resume = deferred()
    let chunkPath = ''
    const splitter = new StreamSplitter({chunkSize: 100, directory})
    splitter.on('chunkFinished', async ({path}) => {
      chunkPath = path
      started.resolve()
      await resume.promise
    })
    splitter.on('error', () => {})
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.end(Buffer.from('payload'))
    await started.promise
    splitter.destroy(new Error('aborted'))
    resume.resolve()
    await closed

    assert.equal(await fileSystem.readFile(chunkPath, 'utf8'), 'payload')
  })

  it('cleans up a write error when autoDestroy is disabled', async () => {
    const realAppend = fileSystem.appendFile
    const failure = new Error('write failed')
    const chunkError = deferred()
    let handle: fs.promises.FileHandle | undefined
    fileSystem.open = (async (...args: Parameters<typeof open>) => {
      handle = await open(...args)
      return handle
    }) as typeof open
    fileSystem.appendFile = (async () => {
      throw failure
    }) as typeof realAppend

    const splitter = new StreamSplitter({chunkSize: 100, directory}, {autoDestroy: false})
    let errorPath: string | null = null
    splitter.on('chunkError', (chunkPath) => {
      errorPath = chunkPath
      chunkError.resolve()
    })
    splitter.write(Buffer.from('payload'))
    await chunkError.promise

    const deadline = Date.now() + 1000
    while (handle?.fd !== -1 || (await fileSystem.readdir(directory)).length !== 0) {
      if (Date.now() >= deadline) throw new Error('chunk cleanup did not finish')
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    assert.equal(splitter.destroyed, false)
    assert.ok(errorPath)
  })

  it('keeps a handed-off chunk if a later chunkFinished listener throws', async () => {
    const failure = new Error('listener failed')
    const splitter = new StreamSplitter({chunkSize: 100, directory})
    let chunkPath = ''
    let errorPath: string | null = null
    splitter.on('chunkFinished', ({path}) => {
      chunkPath = path
    })
    splitter.on('chunkFinished', () => {
      throw failure
    })
    splitter.on('chunkError', (path) => {
      errorPath = path
    })
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.end(Buffer.from('payload'))
    await closed

    assert.equal(errorPath, chunkPath)
    assert.equal(await fileSystem.readFile(chunkPath, 'utf8'), 'payload')
  })

  it('keeps a chunk when its only chunkFinished listener throws with autoDestroy disabled', async () => {
    const splitter = new StreamSplitter({chunkSize: 100, directory}, {autoDestroy: false})
    const chunkError = deferred()
    let chunkPath = ''
    let errorPath: string | null = null
    splitter.on('chunkFinished', ({path}) => {
      chunkPath = path
      throw new Error('listener failed')
    })
    splitter.on('chunkError', (path) => {
      errorPath = path
      chunkError.resolve()
    })
    splitter.end(Buffer.from('payload'))
    await chunkError.promise
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.destroy()
    await closed

    assert.equal(errorPath, chunkPath)
    assert.equal(await fileSystem.readFile(chunkPath, 'utf8'), 'payload')
  })

  it('retains its built-in error listener', async () => {
    const splitter = new StreamSplitter({chunkSize: 100, directory})
    splitter.on('chunkStarted', () => {
      throw new Error('start failed')
    })
    const closed = new Promise<void>((resolve) => splitter.once('close', resolve))
    splitter.write(Buffer.from('payload'))
    await closed
    assert.deepEqual(await fileSystem.readdir(directory), [])
  })
})
