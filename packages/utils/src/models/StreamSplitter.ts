/* global BufferEncoding */
import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'
import stream from 'node:stream'

function randomString(size: number) {
  return crypto.randomBytes(size).toString('base64url').slice(0, size)
}

type Options = {
  chunkSize: number
  directory: string
}

type Callback = (error: Error | null) => void

export class StreamSplitter extends stream.Writable {
  directory: Options['directory']
  currentChunkPath: string | null
  currentChunkSize: number
  fileHandle: fs.FileHandle | null
  filenameTemplate: string
  chunkSize: Options['chunkSize']
  part: number
  private activeOperation?: Promise<void>
  private waitingForChunkStart = false
  private cleanupOperation?: Promise<void>
  private handedOffChunkPath: string | null = null

  constructor({chunkSize, directory}: Options, options?: stream.WritableOptions) {
    super(options)
    this.chunkSize = chunkSize
    this.currentChunkPath = null
    this.currentChunkSize = 0
    this.fileHandle = null
    this.directory = directory
    this.filenameTemplate = randomString(10)
    this.part = 0
    // Errors do not trigger _destroy when autoDestroy is disabled.
    this.on('error', () => {
      if (!this.destroyed) {
        this.cleanup().catch((error) => this.destroy(error))
      }
    })
  }

  _write(chunk: Buffer, _: BufferEncoding, callback: Callback) {
    const operation = this.writeData(chunk)
    this.activeOperation = operation
    operation.then(() => callback(null), callback)
  }

  private async writeData(chunk: Buffer): Promise<void> {
    if (this.fileHandle === null) {
      await this._newChunk()
    }
    this.checkDestroyed()

    let overflow = this.currentChunkSize + chunk.length - this.chunkSize
    while (overflow > 0) {
      await this._writeChunk(chunk.subarray(0, chunk.length - overflow))
      this.checkDestroyed()
      await this._finishChunk()
      this.checkDestroyed()
      await this._newChunk()
      this.checkDestroyed()
      chunk = chunk.subarray(chunk.length - overflow, chunk.length)
      overflow = this.currentChunkSize + chunk.length - this.chunkSize
    }

    await this._writeChunk(chunk)
    this.checkDestroyed()
  }

  _final(callback: Callback) {
    const operation = this._finishChunk()
    this.activeOperation = operation
    operation.then(() => callback(null), callback)
  }

  private checkDestroyed() {
    if (this.destroyed) {
      throw new Error('StreamSplitter was destroyed')
    }
  }

  _destroy(error: Error | null, callback: Callback) {
    const waitingForChunkStart = this.waitingForChunkStart
    if (!this.writableFinished) this.emit('chunkCancelled')

    if (waitingForChunkStart) {
      // No file exists yet. A listener may not support cancellation; its
      // eventual completion will release any resource it acquired.
      callback(error)
      return
    }

    this.cleanup().then(
      () => callback(error),
      (cleanupError) => callback(error ?? cleanupError)
    )
  }

  private cleanup(): Promise<void> {
    if (this.cleanupOperation) return this.cleanupOperation

    this.cleanupOperation = (async () => {
      // An open, write, or chunk handoff may still be running after destroy().
      await this.activeOperation?.catch(() => {})
      let failure: Error | undefined
      try {
        if (!this.writableFinished) {
          await this.emitEvent(
            'chunkError',
            this.currentChunkPath ?? this.handedOffChunkPath
          )
        }
      } catch (error) {
        failure = error
      }
      try {
        if (this.fileHandle !== null) await this.fileHandle.close()
      } catch (error) {
        failure ??= error
      }
      this.fileHandle = null
      try {
        if (this.currentChunkPath !== null) {
          await fs.rm(this.currentChunkPath, {force: true})
        }
      } catch (error) {
        failure ??= error
      }
      this.currentChunkPath = null
      this.handedOffChunkPath = null
      if (failure) throw failure
    })()

    return this.cleanupOperation
  }

  async _writeChunk(chunk: Buffer): Promise<void> {
    await fs.appendFile(this.fileHandle as fs.FileHandle, chunk)
    this.currentChunkSize += chunk.length
  }

  async _finishChunk(): Promise<void> {
    if (this.fileHandle === null) {
      return
    }

    await this.fileHandle.close()
    this.checkDestroyed()

    const chunkPath = this.currentChunkPath
    this.fileHandle = null
    this.currentChunkPath = null
    this.handedOffChunkPath = chunkPath
    await this.emitEvent('chunkFinished', {
      path: chunkPath,
      size: this.currentChunkSize,
    })

    this.handedOffChunkPath = null
    this.currentChunkSize = 0
    this.part += 1
  }

  async emitEvent<T>(name: string, payload: T) {
    const listeners = this.listeners(name)
    for (const listener of listeners) {
      await listener(payload)
    }
  }

  async _newChunk(): Promise<void> {
    const currentChunkPath = path.join(
      this.directory,
      `${this.filenameTemplate}-${this.part}`
    )
    this.waitingForChunkStart = true
    try {
      await this.emitEvent('beforeChunkStarted', currentChunkPath)
    } finally {
      this.waitingForChunkStart = false
      if (this.destroyed) {
        await this.emitEvent('chunkError', null)
      }
    }
    this.checkDestroyed()
    this.fileHandle = await fs.open(currentChunkPath, 'w')
    this.currentChunkPath = currentChunkPath
    this.checkDestroyed()
    await this.emitEvent('chunkStarted', currentChunkPath)
    this.checkDestroyed()
    this.currentChunkSize = 0
  }
}
