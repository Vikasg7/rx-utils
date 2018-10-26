const Rx = require("rxjs")
const R = require("ramda")
const FS = require("fs")
const CP = require("child_process")
const getTempFile = require("tempfile")
const { map, flatMap, concatMap } = require("rxjs/operators")
const { log } = require("console")

const tap = (fn) => {
   const tapper = (x, i) => {
      const v = fn(x, i)
      return (
         (v instanceof Rx.Observable)
         ? v.pipe(map(() => x))
         : (v instanceof Promise)
            ? Rx.from(v).pipe(map(() => x))
            : Rx.of(x)
      )
   }
   return concatMap(tapper)
}

const tapOnFirst = (fn, ...args) => tap((x, i) => i == 0 ? fn(...args) : null)

const tapOnComplete = (fn) => R.pipe(
   reduce(R.always, null),
   tap(fn)
)

const writeFile = (path) => (source) => Rx.Observable.create((Observer) => {
   let writeStream
   let count = 0

   const createStream = () => {
      writeStream = FS.createWriteStream(path, "utf-8")
      writeStream.on('error', (e) => Observer.error(e))
   }

   const onNext = (v) => {
      if (count === 0) { createStream() }
      writeStream.write(v)
      count++
   }

   const onError = (e) => {
      if (writeStream) { writeStream.end() }
      Observer.error(e)
   }

   const onComplete = () => {
      if (writeStream) {
         writeStream.end("", null, () => {
            Observer.next()
            Observer.complete()
         })
      }
   }

   const sub = source.subscribe(onNext, onError, onComplete)
   return () => sub.unsubscribe()
})

const makeCsvRow = R.pipe(
   R.map(R.pipe(
      R.ifElse(R.isNil, R.always(""), R.identity),
      x => x.toString(),
      R.replace(/"/g, '""'),
   )),
   R.join('","'),
   (a) => `"${a}"\r\n`
)

const writeCsv = (path) => {
   const tempPath = getTempFile(".csv")

   const headers = []

   const makeRow = (raw) => {
      const row = []
      for (const key in raw) {
         let i = headers.indexOf(key)
         if (i < 0) {
            i = headers.length
            headers.push(key)
         }
         row[i] = raw[key]
      }
      return row
   }

   const appendFile = (src, dest) => CP.execSync(`cat "${src}" >> "${dest}"`)

   const writeOutput = () => {
      FS.writeFileSync(path, makeCsvRow(headers), "utf-8")
      appendFile(tempPath, path)
   }

   return R.pipe(
      map(makeRow),
      map(makeCsvRow),
      writeFile(tempPath),
      tapOnComplete(writeOutput)
   )
}

const CreateWriter = (path) => {
   const S = new Rx.Subject()
   const O = S.pipe(writeCsv(path))
   // Starts listening to O
   O.subscribe()
   return S
}

const makeReqAsStream = (session) => (options) => Rx.Observable.create((observer) => {
   if (typeof options == "string") options = { url: options };
   const req = session(options)
      .on("error", (e) => observer.error(e))
      .on("data", (d) => observer.next(d))
      .on("complete", () => observer.complete())
      .on("response", (resp) =>
         resp.statusCode >= 400
         ? observer.error(resp.statusCode + ": " + resp.statusMessage)
         : null
      )
   return () => req.abort()
})

const createReqMaker = (session) => (options) => Rx.Observable.create((o) => {
   if (typeof options == "string") options = { url: options };
   const req = session(options, (err, resp, body) => {
      if (err) {
         o.error(err.code)
      }
      else if (resp.statusCode >= 400) {
         o.error(resp.statusCode + ": " + resp.statusMessage)
      }
      else {
         o.next(body)
         o.complete()
      }
   })
   .on("error", (e) => o.error(e))
   return () => req.abort()
})

const parseXml = (tag) => {
   let rest = ""
   const reg = new RegExp(`<${tag}>.*<\/${tag}>`, "gi")
   const cTag = `</${tag}>`

   const parser = (xml) => {
      const haystack = rest.concat(xml)
      rest = haystack.split(cTag).pop()
      return haystack.match(reg)
   }

   return parser
}

const DropLastN = (count) => (source) => Rx.Observable.create((Observer) => {
   let bucket = []

   const onNext = (v) => {
      bucket.push(v)
      if (bucket.length > count) {
         Observer.next(bucket.shift())
      }
   }

   const onError = (err) => Observer.error(err)

   const onComplete = () => {
      bucket = null
      Observer.complete()
   }

   const sub = source.subscribe(onNext, onError, onComplete)

   return sub
})


module.exports = {
   tap,
   writeCsv,
   makeCsvRow,
   writeFile,
   tapOnFirst,
   tapOnComplete,
   CreateWriter,
   createReqMaker,
   makeReqAsStream,
   parseXml,
   makeCsvRow,
   DropLastN
}