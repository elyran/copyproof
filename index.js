const fs = require('fs')
const path = require('path')

const async = require('async')
const options = require('minimist')(process.argv.slice(2))

if(( ! options.dir ) || ( 0 == options._.length ))
	return console.log(`Valid arguments: [--chunk=512] [--tasks=100] --dir=<destination-directory> <files..> \n   (Currently supports only 1 file)`)

const PARALLEL_TASK_LIMIT = options.tasks || 100
const CHUNK_SIZE = options.chunk || 512

const inputFileName = options._[0]
const outputFileName = path.resolve(options.dir, path.basename(inputFileName))
console.log(`Copying ${inputFileName} to ${outputFileName}`)

const fdInput = fs.openSync(inputFileName, 'r')
const fstat = fs.fstatSync(fdInput)
const fileSize = fstat.size
fs.closeSync(fdInput)
// console.log(`File size: ${fileSize}`)

const fdOutput = fs.openSync(outputFileName, 'w') // Open it now so it's locked until we write into it

const LAST_CHUNK_SIZE = (fileSize % CHUNK_SIZE)

let status = {
	processedBytes: 0,
	processedPercent: 0,
	readBytesOk: 0,
	readBytesError: 0,
	readChunksOk: 0,
	readChunksError: 0,
}

const outputInterval = setInterval(() => {
	console.log(`Processed ${status['processedPercent']}%, with ${status['readChunksError']} chunk errors`)
}, 1000)

const requiredIterations = Math.ceil(fileSize / CHUNK_SIZE)

const initialIterationPosition = 0
const tasks = []
for(let i = 0; i < requiredIterations; i++) {
	const myPosition = initialIterationPosition + (i * CHUNK_SIZE)
	const endOfMyPosition = myPosition + CHUNK_SIZE
	const myChunkSize = ((requiredIterations - 1) == i ? LAST_CHUNK_SIZE : CHUNK_SIZE)
	// console.log(`fileSize ${fileSize} endOfMyPosition ${endOfMyPosition} myChunkSize ${myChunkSize}`)
	const task = (callback) => {
		const buffer = Buffer.allocUnsafe(myChunkSize)
		const myFd = fs.openSync(inputFileName, 'r') // We open out own because otherwise the file gets locked, even other apps can't access this file until it's released.
		fs.read(myFd, buffer, 0, myChunkSize, myPosition, (err, bytesRead, buffer) => {
			fs.closeSync(myFd)

			// console.log(buffer.toString('utf8'))

			status['processedBytes'] += myChunkSize
			status['processedPercent'] = (status['processedBytes'] * 100) / fileSize

			if(err) {
				console.log('Error reading:', err, bytesRead, buffer, myChunkSize, myPosition)
				status['readChunksError'] ++
				status['readBytesError'] += myChunkSize
				// reject(err) // << NOT passing an error, but just:
				callback(null) // .. resolving with a `null` so it's clear that has been an eror
			}
			else {
				status['readChunksOk'] ++
				status['readBytesOk'] += myChunkSize
				callback(null, buffer)
			}
		})
	}
	tasks.push(task)
}

async.parallelLimit(tasks, PARALLEL_TASK_LIMIT, (err, buffers) => {
	const buffersWithoutErrors = buffers.filter((element) => {
	    return Buffer.isBuffer(element)
	})
	const numberOfFailedToReadBuffers = buffers.length - buffersWithoutErrors.length
	// const totalSize = CHUNK_SIZE * buffersWithoutErrors.length
	const totalSize = buffersWithoutErrors.reduce((previous, current) => {
		return previous + current.length
	}, 0)

	const finalStatus = {
		buffers : buffers.length,
		numberOfFailedToReadBuffers,
		totalSize,
		lostBytes: totalSize - fileSize,
		lostBytesPercent: 100 - ((totalSize * 100) / fileSize)
	}
	console.log(finalStatus)

	const finalBuffer = Buffer.concat(buffersWithoutErrors, totalSize)
	// console.log(finalBuffer.toString('utf8'))

	fs.writeSync(fdOutput, finalBuffer, 0, finalBuffer.length)
	fs.closeSync(fdOutput)

	clearInterval(outputInterval)
})
